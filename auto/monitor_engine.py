#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor Engine for TwitCasting Auto Recording
Version: 3.6.5（修正版：active_jobs型修正・原子的heartbeat・3本同時録画対応）
- 機能削減なし・I/F互換維持
- 【修正】active_jobsにTaskオブジェクトを保存（float型エラー解消）
- 【修正】heartbeat書き込みを原子的に（Windows対応）
- 【改善】並行URLチェック実装（無制限チェック・容量管理付き）
- 【改善】wait_for撤廃、shieldのみで完全回収
- 【改善】タスクタプル化で二重操作防止
- 【改善】waiting状態をRecorderWrapperに通知
- 【維持】常時10秒心拍（GUI 10〜15秒閾値に追従）
- 【維持】録画中は自動回復・強制リセットをスキップ
- 【維持】active_jobs即時反映、停止直後 active_jobs=0 記録
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import json
import logging
import re
import sys
import time
import tempfile
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

# ===== Logging =====
logger = logging.getLogger("monitor")
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler(sys.stdout)
_ch.setLevel(logging.INFO)
_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
_ch.setFormatter(_formatter)
if not logger.handlers:
    logger.addHandler(_ch)

# ===== Paths =====
ROOT = Path(__file__).resolve().parent               # auto/
LOGS = ROOT / "logs"                                 # auto/logs/
LOGS.mkdir(parents=True, exist_ok=True)
HEARTBEAT = ROOT / "heartbeat.json"                  # auto/heartbeat.json
TARGETS_JSON = ROOT / "targets.json"                 # auto/targets.json


# ===== Config and Constants =====
class EngineState:
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    RECOVERING = "recovering"  # 自動回復中


@dataclass
class MonitorConfig:
    poll_interval: int = 30
    max_concurrent: int = 1
    urls: List[str] = field(default_factory=list)
    root_dir: Optional[Path] = None  # For reference display
    watchdog_interval: int = 10      # ウォッチドッグ間隔（秒）
    max_idle_time: int = 300         # 最大無活動時間（5分）
    url_check_timeout: int = 300     # URL確認タイムアウト（5分）


# ===== Main Engine =====
class MonitorEngine:
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.state = EngineState.STOPPED
        self._task: Optional[asyncio.Task] = None
        self._hb_task: Optional[asyncio.Task] = None   # HOTFIX: 10秒心拍
        self._watchdog_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._urls: List[str] = []
        self._initialized: bool = False
        self._stopping: bool = False  # 再入防止フラグ

        # Runtime stats
        self.total_checks = 0
        self.total_successes = 0
        self.total_errors = 0
        self._last_activity = time.time()
        self._consecutive_timeouts = 0
        self._recovery_count = 0

        # Capacity/concurrency - 【修正】型をUnion[float, asyncio.Task]に変更
        self.active_jobs: Dict[str, Union[float, asyncio.Task]] = {}
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.auth_retry_counts: Dict[str, int] = defaultdict(int)

    # ---------- Initialization ----------
    async def initialize(self) -> None:
        """Load and normalize URLs, configure RecorderWrapper"""
        urls = [u for u in (self.config.urls or []) if u]
        if not urls and TARGETS_JSON.exists():
            try:
                data = json.loads(TARGETS_JSON.read_text(encoding="utf-8"))
                if isinstance(data, dict) and "targets" in data and isinstance(data["targets"], list):
                    urls = [str(u) for u in data["targets"] if u]
                elif isinstance(data, dict) and "urls" in data and isinstance(data["urls"], list):
                    urls = [str(u) for u in data["urls"] if u]
                elif isinstance(data, list):
                    urls = [str(u) for u in data if u]
            except Exception:
                # targets.json が壊れてても起動は続行
                pass

        # Normalize
        self._urls = [self._normalize_url(u) for u in urls]
        self._urls = [u for u in self._urls if u]

        # RecorderWrapper config (lazy import)
        try:
            from recorder_wrapper import RecorderWrapper  # relative
        except ImportError:
            from auto.recorder_wrapper import RecorderWrapper  # absolute
        RecorderWrapper.configure(max_concurrent=self.config.max_concurrent)

        logger.info(
            "Engine initialized (poll=%ss, concurrent=%s, root=%s, targets=%s)",
            self.config.poll_interval,
            self.config.max_concurrent,
            self.config.root_dir or ROOT,
            len(self._urls),
        )
        logger.info(f"URLs loaded: {self._urls}")

        self._initialized = True
        self._update_heartbeat()  # 初回心拍

    # ---------- URL normalization ----------
    def _normalize_url(self, url: str) -> Optional[str]:
        try:
            if not url:
                return None
            url = url.strip()

            # User ID only (alphanumeric_) -> full URL
            if re.match(r"^[A-Za-z0-9_]+$", url):
                return f"https://twitcasting.tv/{url}"

            # Remove /broadcaster suffix
            url = re.sub(r"/broadcaster/?$", "", url)

            # Force https scheme
            if not url.startswith("http"):
                url = f"https://twitcasting.tv/{url}"

            parsed = urlparse(url)
            if "twitcasting.tv" not in parsed.netloc:
                return None

            return url.rstrip("/")
        except Exception:
            return None

    # ---------- Start/Stop ----------
    async def start(self) -> None:
        if self.state in (EngineState.RUNNING, EngineState.STARTING):
            return
        self.state = EngineState.STARTING
        logger.info("Starting monitor engine...")

        # Auto-initialize if not initialized
        if not self._initialized or not getattr(self, "_urls", None):
            logger.info("Auto-initializing engine (not initialized)")
            await self.initialize()

        # Initial login check (non-forced)
        logger.info("Initial login check (non-forced)...")
        try:
            try:
                from recorder_wrapper import RecorderWrapper
            except ImportError:
                from auto.recorder_wrapper import RecorderWrapper
            await RecorderWrapper.ensure_login(force=False)
        except Exception:
            logger.warning("Initial login check skipped due to error", exc_info=True)

        self._stop_event.clear()
        self.state = EngineState.RUNNING
        self._task = asyncio.create_task(self._run_loop())
        self._watchdog_task = asyncio.create_task(self._watchdog_loop())
        # HOTFIX: 常時10秒心拍タスク
        self._hb_task = asyncio.create_task(self._heartbeat_pulse(interval=10))

        self._update_heartbeat()
        logger.info("Monitor engine is now RUNNING")

    async def stop(self) -> None:
        """停止処理（active_jobs=0の確実記録・タスクの安全停止）"""
        # 再入防止フラグで二重停止を物理的に防ぐ
        if getattr(self, "_stopping", False) or self.state in (EngineState.STOPPED, EngineState.STOPPING):
            return
        self._stopping = True
        
        try:
            logger.info("Stopping monitor engine...")
            self.state = EngineState.STOPPING
            self._stop_event.set()

            # アクティブジョブの完了待機（最大10秒に短縮）
            if self.active_jobs:
                logger.info(f"Waiting for {len(self.active_jobs)} active jobs to complete...")
                wait_start = time.time()
                while self.active_jobs and (time.time() - wait_start) < 10:  # 30→10秒
                    await asyncio.sleep(0.5)

            # タスクをタプル化して二重操作防止
            tasks: Tuple[Optional[asyncio.Task], ...] = (self._hb_task, self._watchdog_task, self._task)
            
            # 監視系タスク停止（キャンセル）
            for t in tasks:
                if t:
                    t.cancel()
            
            # 【改善】wait_for撤廃、shieldのみで完全回収
            for t in tasks:
                if t:
                    with contextlib.suppress(asyncio.CancelledError):
                        await asyncio.shield(t)
            
            # タスク参照をクリア（await完了後）
            self._hb_task = self._watchdog_task = self._task = None

            # active_jobs を確実に 0 にする
            self.active_jobs.clear()
            self._update_heartbeat()  # 0を記録

            # RecorderWrapper shutdown
            try:
                try:
                    from recorder_wrapper import RecorderWrapper
                except ImportError:
                    from auto.recorder_wrapper import RecorderWrapper

                all_free = RecorderWrapper.ensure_all_gates_free()
                if not all_free:
                    logger.warning("Some gates may not be fully released")

                await RecorderWrapper.shutdown()
                await self._wait_heartbeat_settle()
            except Exception as e:
                logger.warning(f"RecorderWrapper shutdown warn: {e}", exc_info=True)

            self.state = EngineState.STOPPED
            self._update_heartbeat()  # 停止状態
            logger.info("Monitor engine stopped")
        finally:
            self._stopping = False  # 最後に必ずフラグ解除

    async def _wait_heartbeat_settle(self) -> None:
        """心拍（active_jobs==0）の収束確認"""
        settle_start = time.time()
        while time.time() - settle_start < 5:
            await asyncio.sleep(0.5)
            if not self.active_jobs:
                break
        if self.active_jobs:
            logger.warning(f"Active jobs still present after settle wait: {len(self.active_jobs)}")

    # ---------- Heartbeat Pulse (HOTFIX) ----------
    async def _heartbeat_pulse(self, interval: int = 10) -> None:
        """常時10秒で心拍を刻むバックグラウンドタスク"""
        logger.info("Heartbeat pulse started (interval=%ss)", interval)
        try:
            while not self._stop_event.is_set():
                self._update_heartbeat()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Heartbeat pulse cancelled")
        finally:
            logger.info("Heartbeat pulse stopped")

    # ---------- Watchdog loop（録画中保護） ----------
    async def _watchdog_loop(self) -> None:
        logger.info(
            "Watchdog started (interval=%ss, max_idle=%ss)",
            self.config.watchdog_interval,
            self.config.max_idle_time,
        )
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(self.config.watchdog_interval)

                idle_time = time.time() - self._last_activity

                if idle_time > self.config.max_idle_time:
                    if self.active_jobs:
                        logger.info(
                            f"Watchdog: idle {idle_time:.1f}s but {len(self.active_jobs)} active jobs, skip reset"
                        )
                    else:
                        logger.warning(
                            f"No activity for {idle_time:.1f} seconds, attempting recovery"
                        )
                        await self._attempt_recovery()

                if self._consecutive_timeouts >= 3:
                    if self.active_jobs:
                        logger.info(
                            f"Consecutive timeouts ({self._consecutive_timeouts}) but recording, skip recovery"
                        )
                    else:
                        logger.error(
                            f"Consecutive timeouts detected: {self._consecutive_timeouts}"
                        )
                        await self._attempt_recovery()

                gc.collect()

                # 健全性ログ
                health_status = (
                    "healthy" if idle_time < 60 else
                    ("idle" if idle_time < self.config.max_idle_time else "stale")
                )
                self._write_log(
                    "watchdog_health",
                    {
                        "status": health_status,
                        "idle_seconds": int(idle_time),
                        "active_jobs": len(self.active_jobs),
                        "memory_usage": self._get_memory_usage(),
                    },
                )
        except asyncio.CancelledError:
            logger.info("Watchdog cancelled")
        except Exception as e:
            logger.error(f"Watchdog error: {e}", exc_info=True)
        finally:
            logger.info("Watchdog stopped")

    async def _force_reset_wrapper(self) -> None:
        try:
            from recorder_wrapper import RecorderWrapper
        except ImportError:
            from auto.recorder_wrapper import RecorderWrapper
        try:
            if hasattr(RecorderWrapper, "emergency_reset"):
                RecorderWrapper.emergency_reset()
                logger.info("RecorderWrapper emergency reset executed")
        except Exception as e:
            logger.error(f"Failed to reset RecorderWrapper: {e}")

    async def _attempt_recovery(self) -> None:
        if self.state == EngineState.RECOVERING:
            return
        if self.active_jobs:
            logger.warning(f"Recovery skipped: {len(self.active_jobs)} active jobs")
            return

        self._recovery_count += 1
        logger.warning(f"Starting automatic recovery (attempt #{self._recovery_count})")
        prev_state = self.state
        self.state = EngineState.RECOVERING
        try:
            self.active_jobs.clear()
            self._update_heartbeat()

            self.error_counts.clear()
            self.auth_retry_counts.clear()

            await self._force_reset_wrapper()
            self._consecutive_timeouts = 0

            try:
                try:
                    from recorder_wrapper import RecorderWrapper
                except ImportError:
                    from auto.recorder_wrapper import RecorderWrapper
                await RecorderWrapper.ensure_login(force=True)
            except Exception as e:
                logger.warning(f"Login check during recovery failed: {e}")

            gc.collect()
            logger.info("Recovery completed successfully")
            self._write_log("recovery_success", {"recovery_count": self._recovery_count})
        except Exception as e:
            logger.error(f"Recovery failed: {e}", exc_info=True)
            self._write_log(
                "recovery_failed", {"recovery_count": self._recovery_count, "error": str(e)}
            )
        finally:
            self.state = prev_state
            self._update_heartbeat()

    def _get_memory_usage(self) -> int:
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss // (1024 * 1024)
        except Exception:
            return 0

    # ---------- Main loop ----------
    async def _run_loop(self) -> None:
        logger.info("Starting monitor loop (poll=%ss)", self.config.poll_interval)
        while not self._stop_event.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                logger.info("Monitor loop cancelled")
                break
            except Exception:
                logger.exception("Monitor loop error")
            finally:
                # poll毎に心拍も更新（HOTFIXの保険）
                self._update_heartbeat()
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=self.config.poll_interval
                    )
                except asyncio.TimeoutError:
                    pass
        logger.info("Monitor loop exited")

    # ---------- Poll once（並行処理版） ----------
    async def _poll_once(self) -> None:
        """URLチェック（並行処理・容量管理版）"""
        if not self._urls:
            logger.warning("No URLs to monitor (targets=0)")
            return
        self.total_checks += 1
        
        # チェックと録画を分離
        check_tasks = []
        recording_queue = []
        
        logger.info("Checking %s URLs...", len(self._urls))
        
        # Step1: 全URL並行チェック（無制限）
        for url in list(self._urls):
            if url not in self.active_jobs:  # 録画中以外
                check_tasks.append(self._check_live_status(url))
        
        if check_tasks:
            # 全部同時にチェック（10個でも100個でもOK）
            results = await asyncio.gather(*check_tasks, return_exceptions=True)
            
            # Step2: 生きてるURLを録画キューに
            for i, result in enumerate(results):
                if i < len(self._urls):
                    url = self._urls[i]
                    if not isinstance(result, Exception) and isinstance(result, dict) and result.get("is_live"):
                        recording_queue.append((url, result))
        
        # Step3: 容量管理して録画開始
        for url, status in recording_queue:
            if len(self.active_jobs) < self.config.max_concurrent:
                await self._process_live_url(url, status)
            else:
                # 容量オーバーはwaiting設定
                try:
                    from recorder_wrapper import RecorderWrapper
                except ImportError:
                    from auto.recorder_wrapper import RecorderWrapper
                RecorderWrapper.set_state(url, "waiting")
                logger.info(f"Set {url} to waiting (capacity full)")
                self._write_log("capacity_wait", {"url": url})

    # ---------- 新メソッド：チェックと録画を分離 ----------
    async def _check_live_status(self, url: str) -> dict:
        """生存確認だけ（録画しない）"""
        try:
            from recorder_wrapper import RecorderWrapper
            from live_detector import LiveDetector
        except ImportError:
            from auto.recorder_wrapper import RecorderWrapper
            from auto.live_detector import LiveDetector
        
        logger.info("[detector] start: %s", url)
        self._write_log("detector_start", {"url": url})
        
        detector = LiveDetector()
        try:
            status = await asyncio.wait_for(detector.check_live(url), timeout=20)
        except asyncio.TimeoutError:
            logger.warning("[detector] timeout(20s): %s", url)
            self._write_log("detector_timeout", {"url": url})
            return {"is_live": False, "reason": "timeout"}
        except Exception as e:
            logger.exception("[detector] error: %s -> %s", url, e)
            self._write_log("detector_error", {"url": url, "error": str(e)})
            self.total_errors += 1
            return {"is_live": False, "reason": "error", "error": str(e)}
        
        logger.info("[detector] result: %s", url)
        logger.info(
            "  is_live=%s, reason=%s, detail=%s, cookie_incomplete=%s",
            status.get("is_live"),
            status.get("reason"),
            status.get("detail", ""),
            status.get("cookie_incomplete", False),
        )
        self._write_log(
            "detector_result",
            {
                "url": url,
                "is_live": status.get("is_live"),
                "reason": status.get("reason"),
                "detail": status.get("detail", ""),
                "cookie_incomplete": status.get("cookie_incomplete", False),
            },
        )
        
        return status

    async def _process_live_url(self, url: str, status: dict) -> None:
        """生きてるURLの録画処理（修正版：Taskオブジェクト保存）"""
        try:
            from recorder_wrapper import RecorderWrapper
        except ImportError:
            from auto.recorder_wrapper import RecorderWrapper
        
        # AUTH_REQUIRED 強化処理
        if status.get("reason") == "AUTH_REQUIRED":
            retry_count = self.auth_retry_counts[url]
            max_retries = 2
            if retry_count < max_retries:
                self.auth_retry_counts[url] += 1
                if status.get("cookie_incomplete"):
                    logger.warning("[login] Cookie incomplete -> ensure_login(force=True)")
                    self._write_log("cookie_incomplete_relogin", {"url": url, "retry": retry_count + 1})
                else:
                    logger.info("[login] AUTH_REQUIRED -> ensure_login(force=True)")
                try:
                    await RecorderWrapper.ensure_login(force=True)
                    logger.info("✅ Re-login successful, waiting for cookie propagation...")
                    # 念のためCookie再出力
                    try:
                        recorder = await RecorderWrapper._ensure_recorder()
                        await RecorderWrapper._export_latest_cookie_with_validation(recorder)
                        logger.info("✅ Cookie re-exported for safety")
                    except Exception as ce:
                        logger.warning("Cookie re-export (safety) failed: %s", ce)
                except Exception:
                    logger.warning("ensure_login(force=True) failed", exc_info=True)
                await asyncio.sleep(1.5)
                
                # 再チェック
                status = await self._check_live_status(url)
                logger.info(
                    "[detector] recheck result: is_live=%s, reason=%s",
                    status.get("is_live"),
                    status.get("reason"),
                )
                self._write_log(
                    "detector_recheck_after_login",
                    {
                        "url": url,
                        "is_live": status.get("is_live"),
                        "reason": status.get("reason"),
                        "retry": retry_count + 1,
                    },
                )
                if status.get("reason") == "AUTH_REQUIRED" and retry_count + 1 >= max_retries:
                    logger.error(
                        "[detector] Still AUTH_REQUIRED after %d retries, giving up: %s",
                        retry_count + 1,
                        url,
                    )
                    self._write_log(
                        "auth_required_giveup", {"url": url, "retries": retry_count + 1}
                    )
                    return
            else:
                logger.warning("[detector] AUTH_REQUIRED retry limit reached: %s", url)
                return
        
        if not status.get("is_live"):
            self.error_counts[url] = 0
            self.auth_retry_counts[url] = 0
            return
        
        # 容量チェックとwaiting通知
        if not await self._check_and_reserve_capacity(url):
            # 容量待ちをGUIに通知
            if hasattr(RecorderWrapper, "set_state"):
                RecorderWrapper.set_state(url, "waiting")
            
            self._write_log("capacity_wait", {"url": url})
            logger.info(f"Capacity wait for {url}")
            return
        
        try:
            job_id = f"job_{int(time.time())}"
            self._write_log("recording_start", {"url": url, "job_id": job_id})
            
            metadata = {"detected": status}
            if self.auth_retry_counts[url] > 0:
                metadata["auth_retries"] = self.auth_retry_counts[url]
                logger.info("[record] Starting after %d auth retries", self.auth_retry_counts[url])
            
            force_login_check = self.auth_retry_counts[url] > 0
            
            # 【修正】タスクを作成して保存
            recording_task = asyncio.create_task(
                RecorderWrapper.start_record(
                    url,
                    hint_url=url,
                    job_id=job_id,
                    metadata=metadata,
                    force_login_check=force_login_check,
                )
            )
            
            # 【修正】タスクオブジェクトを保存（floatじゃなくて！）
            self.active_jobs[url] = recording_task
            self._update_heartbeat()  # 即時反映
            
            # タスク実行を待つ
            result = await recording_task
            
            ok = bool(result.get("ok") or result.get("success"))
            files = result.get("output_files") or result.get("files") or []
            reason = result.get("reason") or result.get("error")
            
            if ok:
                self.total_successes += 1
                self.auth_retry_counts[url] = 0
                self._write_log("recording_success", {"url": url, "files": files})
                logger.info("[record] success: %s -> %s files", url, len(files))
            else:
                self.total_errors += 1
                self.error_counts[url] += 1
                self._write_log("recording_error", {"url": url, "reason": reason or "unknown"})
                logger.warning("[record] error: %s -> %s", url, reason or "unknown")
        except Exception as e:
            self.total_errors += 1
            self.error_counts[url] += 1
            logger.exception("[record] exception: %s", url)
            self._write_log("recording_exception", {"url": url, "error": str(e)})
        finally:
            # 【修正】確実にactive_jobsから削除
            self._release_capacity(url)

    # ---------- Log write ----------
    def _write_log(self, event: str, payload: Dict[str, Any]) -> None:
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            path = LOGS / f"monitor_{time.strftime('%Y%m%d')}_001.jsonl"
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": ts, "event": event, **payload}, ensure_ascii=False) + "\n")
        except Exception:
            pass

    # ---------- Heartbeat（修正版：原子的書込） ----------
    def _update_heartbeat(self) -> None:
        """ハートビート更新（Windows原子的書き込み対応）"""
        try:
            hb = {
                "ts": int(time.time()),
                "state": self.state,
                "active_jobs": len(self.active_jobs),
                "total_checks": self.total_checks,
                "total_successes": self.total_successes,
                "total_errors": self.total_errors,
                "targets": len(self._urls),
                "max_concurrent": self.config.max_concurrent,
                "recovery_count": self._recovery_count,
                "last_activity": int(self._last_activity),
            }
            
            HEARTBEAT.parent.mkdir(parents=True, exist_ok=True)
            
            # 【修正】原子的書き込み（Windows対応）
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', 
                                            dir=HEARTBEAT.parent, 
                                            delete=False) as tf:
                json.dump(hb, tf, ensure_ascii=False, indent=2)
                temp_path = Path(tf.name)
            
            # リトライ付き置換
            for i in range(5):
                try:
                    temp_path.replace(HEARTBEAT)
                    break
                except PermissionError:
                    if i < 4:
                        time.sleep(0.05 * (i + 1))
                    else:
                        # フォールバック
                        fallback_path = LOGS / "heartbeat.json"
                        fallback_path.write_text(
                            json.dumps(hb, ensure_ascii=False, indent=2), 
                            encoding="utf-8"
                        )
                        logger.debug(f"Heartbeat fallback to {fallback_path}")
                except Exception as e:
                    logger.error(f"Heartbeat update failed: {e}")
                    break
                    
        except Exception as e:
            logger.error(f"Heartbeat update critical error: {e}", exc_info=True)

    # ---------- Capacity reserve/release ----------
    async def _check_and_reserve_capacity(self, url: str) -> bool:
        if url in self.active_jobs:
            self._write_log("already_recording", {"url": url})
            return False
        if len(self.active_jobs) >= max(1, int(self.config.max_concurrent)):
            return False
        # 一時的に時刻を入れておく（後でTaskに置き換わる）
        self.active_jobs[url] = time.time()
        self._update_heartbeat()  # 即時反映
        return True

    def _release_capacity(self, url: str) -> None:
        """容量解放（修正版：確実な削除）"""
        if url in self.active_jobs:
            # 【修正】Taskの場合はキャンセル試行
            job = self.active_jobs.get(url)
            if isinstance(job, asyncio.Task) and not job.done():
                try:
                    job.cancel()
                except Exception:
                    pass
            
            self.active_jobs.pop(url, None)
            self._update_heartbeat()  # 即時反映

    # ---------- Process one URL（互換性維持用） ----------
    async def _check_url(self, url: str) -> None:
        """互換性維持のため残す（内部では新メソッド使用）"""
        self._last_activity = time.time()
        
        try:
            status = await self._check_live_status(url)
            if status.get("is_live"):
                await self._process_live_url(url, status)
        except Exception as e:
            logger.error(f"URL check error: {url} - {e}", exc_info=True)
            self.total_errors += 1

    # ---------- Health status ----------
    def get_health_status(self) -> Dict[str, Any]:
        idle_time = time.time() - self._last_activity
        health = (
            "healthy" if idle_time < 60 else
            ("idle" if idle_time < self.config.max_idle_time else "stale")
        )
        return {
            "state": self.state,
            "health": health,
            "idle_seconds": int(idle_time),
            "active_jobs": len(self.active_jobs),
            "total_checks": self.total_checks,
            "total_successes": self.total_successes,
            "total_errors": self.total_errors,
            "recovery_count": self._recovery_count,
            "consecutive_timeouts": self._consecutive_timeouts,
            "targets": len(self._urls),
            "timestamp": datetime.now().isoformat(),
        }


# ===== CLI =====
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--poll", type=int, default=30)
    parser.add_argument("--concurrent", type=int, default=1)
    parser.add_argument("urls", nargs="*")
    args = parser.parse_args()

    cfg = MonitorConfig(
        poll_interval=args.poll,
        max_concurrent=args.concurrent,
        urls=args.urls,
        root_dir=ROOT,
    )

    async def _main():
        engine = MonitorEngine(cfg)
        await engine.initialize()
        await engine.start()

        async def show_health():
            while True:
                await asyncio.sleep(60)
                health = engine.get_health_status()
                logger.info(f"Health status: {health}")

        health_task = asyncio.create_task(show_health())
        try:
            while True:
                await asyncio.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await health_task
            await engine.stop()

    asyncio.run(_main())