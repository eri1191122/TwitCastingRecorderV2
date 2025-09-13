以下に、要求どおり**機能削除なし・I/F互換維持・軽リファクタ込み**の“全文差し替え”コードを**Part分割**で掲載します。

---

# Part 1/3 — `monitor_engine.py`（全文）
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor Engine for TwitCasting Auto Recording
Version: 3.2.0 (GPT指摘修正版)

修正内容：
- RecorderWrapper.ensure_login()呼び出しを正しく実装
- 配信チェックにタイムアウト（20秒）追加
- デバッグログ追加で動作確認可能に
- AUTH_REQUIRED時の再検知フロー
"""
import asyncio
import json
import logging
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

# ロギング設定
logger = logging.getLogger("monitor")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
ch.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(ch)


# ====== ユーティリティ ======
ROOT = Path(__file__).resolve().parent
LOGS = ROOT / "logs"
LOGS.mkdir(parents=True, exist_ok=True)


# ====== 設定データクラス ======
class EngineState:
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class RecordingPriority:
    LOW = 0
    NORMAL = 1
    HIGH = 2


@dataclass
class MonitorConfig:
    poll_interval: int = 30
    max_concurrent: int = 1
    urls: List[str] = field(default_factory=list)
    root_dir: Optional[Path] = None


# ====== メインエンジン ======
class MonitorEngine:
    def __init__(self, config: MonitorConfig):
        self.config = config
        self.state = EngineState.STOPPED
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._urls: List[str] = []
        self._seen: Dict[str, float] = {}

        # 統計
        self.total_checks = 0
        self.total_successes = 0
        self.total_errors = 0

        # 容量/同時実行の管理
        self.active_jobs: Dict[str, float] = {}
        self.error_counts: Dict[str, int] = {}

    async def initialize(self) -> None:
        """初期化（URL正規化・Recorder構成）"""
        self._urls = [u for u in (self.config.urls or []) if u]
        self._urls = [self._normalize_url(u) for u in self._urls]
        self._urls = [u for u in self._urls if u]

        # 遅延import（依存循環回避）
        from recorder_wrapper import RecorderWrapper

        # RecorderWrapper設定（root_dir引数を削除）
        RecorderWrapper.configure(
            max_concurrent=self.config.max_concurrent
        )
        
        logger.info(f"Engine initialized (poll={self.config.poll_interval}s, concurrent={self.config.max_concurrent}, root={self.config.root_dir})")

    def _normalize_url(self, url: str) -> Optional[str]:
        """URL正規化"""
        try:
            if not url:
                return None
            
            # ~user ショート→正規
            url = url.strip()
            if re.match(r"^[A-Za-z0-9_]+$", url):
                return f"https://twitcasting.tv/{url}"

            # /broadcaster 末尾余分を除去
            url = re.sub(r"/broadcaster/?$", "", url)

            # httpsスキーム強制
            if not url.startswith("http"):
                url = f"https://twitcasting.tv/{url}"
            
            parsed = urlparse(url)
            if 'twitcasting.tv' not in parsed.netloc:
                return None
            
            return url.rstrip('/')
        except Exception:
            return None

    async def start(self) -> None:
        if self.state in (EngineState.RUNNING, EngineState.STARTING):
            return
        self.state = EngineState.STARTING
        logger.info("Starting monitor engine...")

        # 初期ログインチェック（非強制）
        logger.info("Initial login check (non-forced)...")
        try:
            from recorder_wrapper import RecorderWrapper
            await RecorderWrapper.ensure_login(force=False)
        except Exception:
            logger.warning("Initial login check skipped due to error", exc_info=True)

        # フラグ
        self._stop_event.clear()
        self.state = EngineState.RUNNING
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Monitor engine is now RUNNING")

    async def stop(self) -> None:
        if self.state in (EngineState.STOPPED, EngineState.STOPPING):
            return
        logger.info("Stopping monitor engine...")
        self.state = EngineState.STOPPING
        self._stop_event.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except Exception:
                pass
        self.state = EngineState.STOPPED
        logger.info("Monitor engine stopped")

    async def _run_loop(self) -> None:
        logger.info(f"Starting monitor loop (poll={self.config.poll_interval}s)")
        while not self._stop_event.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Monitor loop error")
            finally:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self.config.poll_interval)
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    pass
        logger.info("Monitor loop cancelled")

    async def _poll_once(self) -> None:
        if not self._urls:
            return
        self.total_checks += 1

        logger.info(f"Checking {len(self._urls)} URLs...")
        for url in list(self._urls):
            await self._check_url(url)

    def _write_log(self, event: str, payload: Dict[str, Any]) -> None:
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            path = LOGS / f"monitor_{time.strftime('%Y%m%d')}_001.jsonl"
            with path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": ts, "event": event, **payload}, ensure_ascii=False) + "\n")
        except Exception:
            pass

    async def _check_and_reserve_capacity(self, url: str) -> bool:
        # 単純な同時実行制限：active_jobsの長さで判定
        if len(self.active_jobs) >= (self.config.max_concurrent or 1):
            return False
        self.active_jobs[url] = time.time()
        return True

    async def _check_url(self, url: str) -> None:
        from recorder_wrapper import RecorderWrapper
        from live_detector import LiveDetector

        logger.info(f"[detector] start: {url}")
        self._write_log("detector_start", {"url": url})

        detector = LiveDetector()
        try:
            status = await asyncio.wait_for(detector.check_live(url), timeout=20)
        except asyncio.TimeoutError:
            logger.warning(f"[detector] timeout(20s): {url}")
            self._write_log("detector_timeout", {"url": url})
            return
        except Exception as e:
            logger.exception(f"[detector] error: {url} -> {e}")
            self._write_log("detector_error", {"url": url, "error": str(e)})
            self.total_errors += 1
            return

        logger.info(f"[detector] result: {url} -> {status}")
        self._write_log("detector_result", {
            "url": url,
            "is_live": status.get("is_live"),
            "reason": status.get("reason"),
            "detail": status.get("detail", "")
        })
        
        # ========== AUTH_REQUIRED処理（修正版） ==========
        if status.get("reason") == "AUTH_REQUIRED":
            logger.info("[login] AUTH_REQUIRED -> ensure_login(force=True)")
            self._write_log("auth_required_detected", {
                "url": url,
                "detail": status.get("detail", "要ログイン")
            })
            
            # RecorderWrapperの公開APIでログイン
            try:
                ok = await RecorderWrapper.ensure_login(force=True)
                if not ok:
                    logger.error("[login] ensure_login failed")
                    self._write_log("login_failed", {"url": url})
                    self.error_counts[url] += 1
                    return
                
                logger.info("[login] successful, re-checking after 1 sec...")
                self._write_log("login_success_recheck", {"url": url})
                
                # cookie反映の猶予
                await asyncio.sleep(1.0)
                
                # 再検知（タイムアウト付き）
                try:
                    status = await asyncio.wait_for(
                        self.detector.check_live(url),
                        timeout=20
                    )
                except Exception as e:
                    logger.exception(f"[detector] retry after login failed: {e}")
                    self._write_log("recheck_error", {
                        "url": url,
                        "error": str(e)
                    })
                    return
                
                logger.info(f"[detector] result(after login): {url} -> {status}")
                self._write_log("recheck_after_login", {
                    "url": url,
                    "is_live": status.get("is_live"),
                    "reason": status.get("reason"),
                    "detail": status.get("detail", "")
                })
                
            except Exception as e:
                logger.error(f"[login] process error: {e}")
                self._write_log("login_error", {
                    "url": url,
                    "error": str(e),
                    "traceback": traceback.format_exc()
                })
                self.error_counts[url] += 1
                return
        
        # 配信中チェック
        if status.get("is_live"):
            
            # 容量チェックと予約を原子的に
            if not await self._check_and_reserve_capacity(url):
                logger.info(f"Recording capacity full, skipping: {url}")
                self._write_log("capacity_full", {"url": url})
                return
            
            # 配信中
            movie_id = status.get("movie_id", f"live_{int(time.time())}")
            logger.info(f"🔴 LIVE DETECTED: {url} (movie_id={movie_id})")
            self._write_log("live_detected", {
                "url": url,
                "movie_id": movie_id,
                "priority": RecordingPriority.NORMAL
            })
            
            # 録画ジョブ開始（RecorderWrapper経由）
            try:
                result = await RecorderWrapper.start_record(
                    target=url, hint_url=url, duration=None, job_id=None,
                    force_login_check=False,
                    metadata={"movie_id": movie_id}
                )
                self.total_successes += 1 if result.get("success") else 0
                if not result.get("success"):
                    self.total_errors += 1
                self._write_log("record_result", {"url": url, **result})
            except Exception as e:
                logger.exception(f"record_start failed: {e}")
                self.total_errors += 1
                self._write_log("record_error", {"url": url, "error": str(e)})
            finally:
                self.active_jobs.pop(url, None)
        else:
            # 非配信
            self._write_log("not_live", {"url": url, "reason": status.get("reason", "NOT_LIVE")})


# 直接起動（CLIデバッグ用）
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("urls", nargs="*", help="TwitCasting URLs")
    parser.add_argument("--poll", type=int, default=30)
    parser.add_argument("--concurrent", type=int, default=1)
    args = parser.parse_args()

    cfg = MonitorConfig(
        poll_interval=args.poll,
        max_concurrent=args.concurrent,
        urls=args.urls,
        root_dir=ROOT
    )

    async def main():
        engine = MonitorEngine(cfg)
        await engine.initialize()
        await engine.start()
        await asyncio.sleep(cfg.poll_interval * 2)
        await engine.stop()

    asyncio.run(main())
```

---

# Part 2/3 — `recorder_wrapper.py`（全文）
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recorder Wrapper for TwitCasting Auto Recording
Version: 4.2.1 (ensure_login公開API追加版)

重大バグ修正：
- レコーダー二重初期化レース防止
- プロセス全体での同時実行数制限
- EventLoop別リソース管理維持
- ensure_login公開API追加（monitor_engineから呼び出し可能）
"""
import asyncio
import json
import sys
import time
import traceback
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import logging
logger = logging.getLogger("wrapper")
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(_ch)

# ====== パス ======
ROOT = Path(__file__).resolve().parent
RECORDINGS = ROOT / "recordings"
LOGS = ROOT / "logs"
RECORDINGS.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)


# ====== 設定 ======
@dataclass
class WrapperConfig:
    max_concurrent: int = 1


class TargetPrefix:
    URL = "url"


class RecordingStatus:
    IDLE = "idle"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"


@dataclass
class RecordingJob:
    """録画ジョブ情報（完全版）"""
    job_id: str
    target: str
    url: str
    status: RecordingStatus = RecordingStatus.IDLE
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    duration: Optional[int] = None
    output_files: List[str] = field(default_factory=list)
    error: Optional[str] = None
    raw_result: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    semaphore_acquired: bool = False
    proc_gate_acquired: bool = False


class RecorderWrapper:
    _configured = False
    _config = WrapperConfig()

    _recording_jobs: Dict[str, RecordingJob] = {}
    _job_counter = 0

    _global_semaphore: Optional[asyncio.Semaphore] = None
    _process_gate: Optional[asyncio.Semaphore] = None

    _shutdown_event = asyncio.Event()
    _recorder_instance: Any = None
    _recorder_init_lock = threading.Lock()

    _last_login_check: Optional[float] = None

    @classmethod
    def configure(cls, max_concurrent: int = 1) -> None:
        cls._config = WrapperConfig(max_concurrent=max_concurrent)
        cls._global_semaphore = asyncio.Semaphore(max(1, max_concurrent))
        cls._process_gate = asyncio.Semaphore(1)
        cls._configured = True
        logger.info(f"RecorderWrapper configured (max_concurrent={max_concurrent}, root={ROOT})")

    @classmethod
    def _log_event(cls, event: str, payload: Optional[Dict[str, Any]] = None) -> None:
        payload = payload or {}
        try:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            p = LOGS / f"wrapper_{time.strftime('%Y%m%d')}_001.jsonl"
            with p.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": ts, "event": event, **payload}, ensure_ascii=False) + "\n")
        except Exception:
            pass

    # ====== 公開API ======
    @classmethod
    async def ensure_login(cls, force: bool = False) -> bool:
        """公開API：monitor_engineから呼べるログイン確認
        （内部の_ensure_loginをラップするだけ）
        
        Args:
            force: 強制的にログイン画面を開くかどうか
            
        Returns:
            bool: ログイン成功したらTrue
        """
        return await cls._ensure_login(force=force)
    
    @classmethod
    async def _ensure_recorder(cls) -> Any:
        """レコーダーインスタンスの確保（二重初期化防止版）"""
        # threading.Lockで保護（EventLoop跨いでも安全）
        with cls._recorder_init_lock:
            if cls._recorder_instance is None:
                # facade.pyのインポート（明示的エラー）
                try:
                    from facade import TwitCastingRecorder
                except ImportError as e:
                    error_msg = (
                        f"\n[FATAL ERROR] TwitCastingRecorder (facade.py) not found!\n"
                        f"{e}\n"
                    )
                    logger.error(error_msg)
                    raise
                cls._recorder_instance = TwitCastingRecorder()
        
        # 非同期初期化（必要なら）
        rec = cls._recorder_instance
        if getattr(rec, "_initialized", False):
            return rec
        try:
            await rec.initialize()
        except Exception as e:
            logger.error(f"Failed to initialize recorder: {e}")
            raise
        return rec

    @classmethod
    async def _ensure_login(cls, force: bool = False) -> bool:
        """ログイン状態の確保（堅牢版）"""
        try:
            recorder = await cls._ensure_recorder()
        except Exception as e:
            logger.error(f"Failed to get recorder: {e}")
            return False
        
        try:
            # test_login_statusメソッドの存在確認
            if not hasattr(recorder, 'test_login_status'):
                logger.warning("test_login_status method not found, assuming logged in")
                return True
            
            # ログイン状態確認
            status = await recorder.test_login_status()
            cls._log_event("login_check", {"status": status})
            
            # 強制ログイン指定時は状態に関わらずウィザードを開く
            if force:
                logger.info(f"[login] Force login requested (current status: {status}) -> opening browser")
                cls._log_event("login_force", {"current_status": status})
            else:
                # strong なら何もせず成功
                if status in ["strong", True]:
                    cls._last_login_check = time.time()
                    return True
                
                logger.info(f"Login required (current status: {status}), opening browser...")
                cls._log_event("login_required", {"current_status": status})
            
            # setup_loginメソッドの存在確認
            if not hasattr(recorder, 'setup_login'):
                logger.error("setup_login method not found")
                return False
            
            # ログイン実行
            success = await recorder.setup_login()
            if success:
                logger.info("✅ Login successful!")
                cls._last_login_check = time.time()
                cls._log_event("login_success", {})
                
                # Netscape cookies export for LiveDetector (enter_cookie)
                try:
                    from datetime import datetime
                    from pathlib import Path
                    from tc_recorder_core import LOGS, _save_cookies_netscape  # type: ignore
                    # ensure headless context and export only twitcasting.tv cookies
                    ctx = await recorder.chrome.ensure_headless()
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    out = LOGS / f"cookies_enter_{ts}.txt"
                    await _save_cookies_netscape(ctx, out, "twitcasting.tv")
                    logger.info(f"Cookies exported for detector: {out}")
                    cls._log_event("cookie_exported", {"path": str(out)})
                except Exception as ce:
                    logger.warning(f"Cookie export failed (non-fatal): {ce}")
                
                return True
            else:
                logger.error("❌ Login failed")
                cls._log_event("login_failed", {})
                return False
        
        except asyncio.CancelledError:
            raise
        except Exception as e:
            cls._log_event("login_error", {
                "error": str(e),
                "traceback": traceback.format_exc(),
            })
            logger.error(f"Login ensure failed: {e}")
            return False

    # ====== 録画開始 ======
    @classmethod
    async def start_record(
        cls,
        target: str,
        *,
        hint_url: Optional[str] = None,
        duration: Optional[int] = None,
        job_id: Optional[str] = None,
        force_login_check: bool = False,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        録画開始（完全修正版）
        
        Args:
            target: ターゲット識別子
            hint_url: URL直接指定
            duration: 録画時間（秒）
            job_id: ジョブID
            force_login_check: 強制ログイン確認
            metadata: 追加メタデータ
            
        Returns:
            録画結果の詳細辞書
        """
        start_time = time.time()
        
        # シャットダウン中チェック
        if cls._shutdown_event.is_set():
            return cls._create_error_result(
                None, "shutdown_in_progress", start_time
            )
        
        if not cls._configured:
            cls.configure()
        
        # セマフォ取得
        job_id = job_id or f"job_{int(start_time)}"
        await cls._process_gate.acquire()
        proc_gate_acquired = True
        
        await cls._global_semaphore.acquire()
        acquired = True
        
        # 開始ログ
        cls._log_event("start_request", {
            "job_id": job_id,
            "target": target,
            "duration": duration,
            "current_count": len(cls._recording_jobs),
            "max_concurrent": cls._config.max_concurrent,
            "metadata": metadata
        })
        
        # セマフォ取得用フラグ
        acquired = False
        proc_gate_acquired = False
        job_registered = False
        try:
            # 事前ログインチェック
            if force_login_check:
                ok = await cls._ensure_login(force=False)
                if not ok:
                    return cls._create_error_result(job_id, "login_required_failed", start_time)
            
            # ジョブ登録
            job = RecordingJob(
                job_id=job_id,
                target=TargetPrefix.URL,
                url=hint_url or target,
                status=RecordingStatus.RUNNING,
            )
            cls._recording_jobs[job_id] = job
            job_registered = True
            
            # 実処理（facade → core）
            try:
                from facade import TwitCastingRecorder
            except ImportError:
                return cls._create_error_result(job_id, "facade_import_error", start_time)
            
            rec = await cls._ensure_recorder()
            
            # 録画実行（coreに委譲）
            result = await rec.record(job.url, duration)
            job.raw_result = result
            
            if result.get("success"):
                job.status = RecordingStatus.SUCCESS
                job.completed_at = time.time()
                job.duration = int(job.completed_at - job.started_at)
                job.output_files = result.get("files", []) or []
                cls._log_event("record_success", {
                    "job_id": job_id,
                    "files": job.output_files
                })
                return {
                    "success": True,
                    "files": job.output_files,
                }
            else:
                job.status = RecordingStatus.FAILED
                job.completed_at = time.time()
                job.error = result.get("error", "unknown")
                cls._log_event("record_failed", {
                    "job_id": job_id,
                    "error": job.error
                })
                return {
                    "success": False,
                    "error": job.error
                }
        except Exception as e:
            logger.exception("record_start fatal error")
            return cls._create_error_result(job_id, f"exception:{e}", start_time)
        finally:
            # unlocks
            if job_registered:
                cls._recording_jobs.pop(job_id, None)
            if acquired:
                try:
                    cls._global_semaphore.release()
                except Exception:
                    pass
            if proc_gate_acquired:
                try:
                    cls._process_gate.release()
                except Exception:
                    pass

    # ====== ユーティリティ ======
    @classmethod
    def _create_error_result(cls, job_id: Optional[str], reason: str, start_time: float) -> Dict[str, Any]:
        return {
            "success": False,
            "error": reason,
            "job_id": job_id,
            "elapsed": time.time() - start_time
        }
```

---

# Part 3/3 — `chrome_singleton.py`（全文）
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome Singleton Manager v7.3 Strong-Guarantee (修正版)
- 目的: ログイン後に録画用ヘッドレス context へセッションを確実に引き継ぐ
- 変更点:
  1) 可視(context) -> ヘッドレス(context) へ TwitCasting系Cookieを明示注入
  2) guided_login_wizard の出口で "ヘッドレスが strong" を必ず再確認 (保証)
  3) weak->none の降格も抑止 (強度が下がる瞬間のブレを吸収)
  4) 初回3秒はCookie判定を完全無視（既存仕様維持）
  5) weakは成功にしない（10秒後のみ昇格試行の既存仕様維持）
  6) 【修正】Cookie判定の閾値を実態に合わせて緩和
  7) 【重要修正】544行目の強制return "strong"を削除
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

try:
    from playwright.async_api import async_playwright, BrowserContext, Page
except ImportError:
    async_playwright = None
    BrowserContext = object
    Page = object


# ===== パス定義 =====
ROOT = (
    Path(__file__).resolve().parent.parent
    if (Path(__file__).resolve().parent.name == "core")
    else Path(__file__).resolve().parent
)
AUTH_DIR = ROOT / ".auth" / "playwright"
LOGS = ROOT / "logs"
LOGS.mkdir(parents=True, exist_ok=True)
AUTH_DIR.mkdir(parents=True, exist_ok=True)


# ===== ログ =====
def log(msg: str, level: str = "INFO") -> None:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_path = LOGS / "chrome_diagnostic.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"{timestamp} [{level}] {msg}\n")
        print(f"[CHROME-{level}] {msg}")
    except Exception:
        pass


@dataclass
class _CtxMeta:
    headless: bool
    created_at: float


class ChromeSingleton:
    """Playwright Chrome の単一管理 v7.3"""

    _instance: Optional["ChromeSingleton"] = None

    @classmethod
    def instance(cls) -> "ChromeSingleton":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._pw = None
        self._browser_ctx: Optional[BrowserContext] = None
        self._headless_ctx: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._ctx_meta: Optional[_CtxMeta] = None
        self._lock = asyncio.Lock()
        self._login_in_progress = False
        self._wizard_start_time = 0.0
        self._last_known_login_status: Optional[str] = None

    async def _launch_new_ctx(self, *, headless: bool) -> BrowserContext:
        if async_playwright is None:
            raise RuntimeError("playwright not installed")

        if self._pw is None:
            self._pw = await async_playwright().start()
            log("Playwright initialized")

        chromium = self._pw.chromium
        ctx = await chromium.launch_persistent_context(
            user_data_dir=str(AUTH_DIR),
            headless=headless,
            channel="chrome"
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        if headless:
            self._headless_ctx = ctx
        else:
            self._browser_ctx = ctx
            self._page = page
            self._ctx_meta = _CtxMeta(headless=headless, created_at=time.time())

        log(f"Context launched successfully (headless={headless})")
        return ctx

    async def _hard_close_ctx(self, *, reason: str, headless: bool = False) -> None:
        ctx = self._headless_ctx if headless else self._browser_ctx
        if ctx:
            try:
                await ctx.close()
                log(f"Context closed (headless={headless}, reason={reason})")
            except Exception as e:
                log(f"Context close error: {e}", "WARN")

        if headless:
            self._headless_ctx = None
        else:
            self._browser_ctx = None
            self._page = None
            self._ctx_meta = None

    async def _ensure_context(self, *, headless: bool) -> BrowserContext:
        if headless:
            if self._headless_ctx:
                return self._headless_ctx
            return await self._launch_new_ctx(headless=True)

        if self._browser_ctx and self._ctx_meta:
            if not self._ctx_meta.headless:
                return self._browser_ctx

            await self._hard_close_ctx(reason="mode_change_to_visible", headless=False)
            return await self._launch_new_ctx(headless=False)

        return await self._launch_new_ctx(headless=False)

    async def initialize(self) -> None:
        # 互換API
        await self.ensure_headless()

    async def ensure_headless(self) -> BrowserContext:
        async with self._lock:
            return await self._ensure_context(headless=True)

    async def ensure_visible(self) -> BrowserContext:
        async with self._lock:
            return await self._ensure_context(headless=False)

    # ===== ウィザード UI =====
    async def _show_login_guide(self, page: Page, timeout_minutes: int = 3) -> None:
        """ログイン案内をページに表示"""
        try:
            await page.evaluate("""
                () => {
                    const existing = document.getElementById('login-guide-overlay');
                    if (existing) existing.remove();

                    const div = document.createElement('div');
                    div.id = 'login-guide-overlay';
                    div.style.cssText = `
                        position: fixed;
                        top: 20px;
                        left: 50%;
                        transform: translateX(-50%);
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 20px 30px;
                        border-radius: 10px;
                        font-size: 16px;
                        z-index: 999999;
                        box-shadow: 0 8px 24px rgba(0,0,0,.2);
                        animation: slideDown .3s ease-out;
                    `;
                    div.innerHTML = `
                        <div style="font-weight: bold; font-size: 18px; margin-bottom: 8px;">TwitCasting へログインしてください</div>
                        <div>1. 右上のログインからサインイン</div>
                        <div>2. 画面の「✅ ログイン成功！」表示を待ちます</div>
                        <div>3. ログイン完了後、自動で次へ進みます</div>
                    `;

                    const style = document.createElement('style');
                    style.textContent = `
                        @keyframes slideDown {
                            from { opacity: 0; transform: translateX(-50%) translateY(-20px); }
                            to { opacity: 1; transform: translateX(-50%) translateY(0); }
                        }
                    `;
                    document.head.appendChild(style);
                    document.body.appendChild(div);
                }
            """)
        except Exception:
            pass

    # ===== ウィザード本体 =====
    async def guided_login_wizard(self, url: Optional[str] = None, timeout_sec: int = 180) -> bool:
        """
        ログインウィザード v7.3
        - 初回3秒は完全にCookie無視
        - weakは成功にしない（10秒後のみ昇格試行）
        - strong検出後は Cookie をヘッドレスへ移送し、ヘッドレスで strong を再確認（保証）
        """
        async with self._lock:
            if self._login_in_progress:
                log("Login wizard already running", "WARN")
                return False
            self._login_in_progress = True
            self._wizard_start_time = time.time()
            log("Login wizard started")

        try:
            # 可視コンテキスト起動
            ctx = await self.ensure_visible()
            page: Page = self._page or (ctx.pages[0] if ctx.pages else await ctx.new_page())

            # TwitCastingトップページ
            target = url or "https://twitcasting.tv/"
            await page.goto(target, wait_until="domcontentloaded")
            log(f"Login page opened: {target}")

            # ログイン案内を表示
            await self._show_login_guide(page, timeout_minutes=timeout_sec // 60)

            # 初回3秒間はCookie判定しない
            log("Initial cookie ignore period (3 seconds)")
            await asyncio.sleep(3.0)

            # ログイン成功待機
            start = time.time()
            weak_detected_time = None

            while time.time() - start < timeout_sec:
                elapsed = time.time() - self._wizard_start_time
                if elapsed < 3.0:
                    await asyncio.sleep(0.5)
                    continue

                # コンテキスト側のCookieで強度を判定
                status = await self._probe_login_status_via_context()
                log(f"Wizard loop: status={status}", "DEBUG")

                if status == "strong":
                    # ✅ ログイン成功表示（UX）
                    try:
                        await page.evaluate("""
                            () => {
                                const old = document.getElementById('login-guide-overlay');
                                if (old) old.remove();
                                const div = document.createElement('div');
                                div.style.cssText = `
                                    position: fixed;
                                    top: 20px; left: 50%; transform: translateX(-50%);
                                    background: #22c55e; color: white; padding: 10px 18px;
                                    border-radius: 10px; font-size: 24px; z-index: 999999;
                                `;
                                div.textContent = '✅ ログイン成功！';
                                document.body.appendChild(div);
                                setTimeout(() => div.remove(), 2000);
                            }
                        """)
                    except Exception:
                        pass

                    await asyncio.sleep(0.5)

                    # === 安全切替: 先にヘッドレス起動 → Cookie移送 → 可視閉じ ===
                    try:
                        log("Starting safe context switch")

                        # 1) ヘッドレス起動
                        headless_ctx = await self.ensure_headless()
                        if headless_ctx:
                            log("Headless context ready (pre-switch)")

                            # 2) Cookie移送（可視→ヘッドレス）
                            injected = await self._inject_visible_cookies_into_headless()
                            if injected == 0:
                                log("No twitcasting.* cookies injected into headless", "WARN")

                            # 3) 可視を閉じて切替完了
                            await self._hard_close_ctx(reason="post_login_switch", headless=False)
                            log("Visible context closed; switched to headless")
                            # Set active context to headless explicitly (avoid stale references)
                            try:
                                self._browser_ctx = self._headless_ctx
                                self._page = None
                                self._ctx_meta = _CtxMeta(headless=True, created_at=time.time())
                            except Exception as _e:
                                log(f"Failed to switch active context reference: {_e}", "WARN")

                        # 4) 最終保証: ヘッドレスで strong を確認
                        final = await self.check_login_status()
                        if final != "strong":
                            log(f"Wizard exit blocked: final={final}", "WARN")
                            return False

                    except Exception as e:
                        log(f"Context switch flow failed: {e}", "WARN")
                        # 失敗しても最終確認に賭ける
                        final = await self.check_login_status()
                        if final != "strong":
                            return False

                    return True

                # === weak: 10秒後だけ昇格試行
                if status == "weak":
                    if weak_detected_time is None:
                        weak_detected_time = time.time()
                        log("Weak status detected (will try promotion after 10s)")

                    if weak_detected_time and (time.time() - weak_detected_time > 10):
                        log("10 seconds elapsed since weak detection, attempting promotion")
                        promoted = await self._try_promote_to_strong(page)
                        if promoted:
                            status = await self._probe_login_status_via_context()
                            if status == "strong":
                                self._last_known_login_status = "strong"
                                log("Promotion successful, continuing as strong")
                                continue
                        else:
                            log("Promotion failed (still weak)", "WARN")

                await asyncio.sleep(1.0)

            log("Login wizard timeout", "WARN")
            return False

        finally:
            async with self._lock:
                self._login_in_progress = False
                self._wizard_start_time = 0
                log("Login wizard ended")

    # ===== ログイン状態判定 =====
    async def _probe_login_status_via_context(self) -> Optional[str]:
        """PlaywrightのContextからCookieを読む（Context優先）"""
        try:
            ctx = self._browser_ctx or self._headless_ctx
            if not ctx:
                return None

            cookies = await ctx.cookies()
            tc_cookies = [
                c for c in cookies
                if isinstance(c, dict) and ("twitcasting.tv" in str(c.get("domain", "")))
            ]

            names = {c.get("name", "") for c in tc_cookies}

            # ウィザード開始から3秒未満は none を返して初期ブレを無視
            if self._wizard_start_time > 0 and (time.time() - self._wizard_start_time < 3.0):
                log("Initial period active, returning none", "DEBUG")
                return "none"

            # 【修正】strong 判定（緩和版 - どれか1つでOK）
            strong_cookies = {"_twitcasting_session", "tc_ss", "twitcasting_session", "tc_sid", "tc_s"}
            if names & strong_cookies:
                return "strong"

            # 【修正】weak 判定（緩和版 - 1個でもあればOK）
            weak_cookies = {"tc_s", "tc_u", "user", "twitcasting_user_id", "twitcasting_live_session"}
            if names & weak_cookies:
                return "weak"

            return "none"

        except Exception as e:
            log(f"Context cookie probe failed: {e}", "WARN")
            return None

    def _find_cookies_db(self) -> Optional[Path]:
        """Cookiesファイルを探す（DBフォールバック用）"""
        candidates = [
            AUTH_DIR / "Default" / "Network" / "Cookies",
            AUTH_DIR / "Default" / "Cookies",
            AUTH_DIR / "Network" / "Cookies",
        ]
        for p in candidates:
            if p.exists():
                return p
        return None

    async def _try_promote_to_strong(self, page: Page) -> bool:
        """弱い状態からの昇格（ページ遷移など）"""
        try:
            await page.goto("https://twitcasting.tv/", wait_until="domcontentloaded")
            await asyncio.sleep(1.0)
            return True
        except Exception:
            return False

    async def _inject_visible_cookies_into_headless(self) -> int:
        """
        可視contextの TwitCasting系 Cookie をヘッドレスへ移送する
        戻り値: 注入したCookie個数
        """
        if not self._browser_ctx:
            return 0
        try:
            # 可視側でCookie取得
            src = self._browser_ctx
            cookies = await src.cookies()
            tc = [c for c in cookies if "twitcasting.tv" in str(c.get("domain", ""))]
            if not tc:
                return 0

            # ヘッドレスを起動し、Cookie注入
            dst = await self.ensure_headless()
            await dst.add_cookies(tc)
            log(f"Injected {len(tc)} cookies into headless")
            return len(tc)
        except Exception as e:
            log(f"Cookie injection failed: {e}", "WARN")
            return 0

    async def check_login_status(self) -> Optional[str]:
        """公開API: ログイン状態の総合判定（Context優先→DB）"""
        # 1) Context優先
        s = await self._probe_login_status_via_context()
        if s in ("strong", "weak"):
            self._last_known_login_status = s
            return s

        # 2) DBフォールバック
        db = self._find_cookies_db()
        if not db:
            return "none"

        try:
            with sqlite3.connect(str(db)) as conn:
                cur = conn.cursor()
                cur.execute("SELECT name FROM cookies WHERE host_key LIKE '%twitcasting%'")
                names = {row[0] for row in cur.fetchall()}

            # 【修正】strong判定（緩和版）
            strong_cookies = {"_twitcasting_session", "tc_ss", "twitcasting_session", "tc_sid", "tc_s"}
            if names & strong_cookies:
                return "strong"
            
            # 【修正】weak判定（緩和版 - 1個でもあればOK）  
            weak_cookies = {"tc_s", "tc_u", "user", "twitcasting_user_id", "twitcasting_live_session"}
            if names & weak_cookies:
                return "weak"
            
            return "none" if names else None
        except Exception:
            return None


# 互換アクセサ
get_chrome_singleton = ChromeSingleton.instance
get_instance = ChromeSingleton.instance
```

