#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor Engine for TwitCasting Recorder
監視→自動録画エンジン
重大バグ修正版
- タスク管理追加（ゾンビ化防止）
- URL正規化統一（c:/g:/ig:対応）
- ディレクトリ作成保証
- 重複起動レース解決
"""
import asyncio
import json
import time
import shutil
from pathlib import Path
from typing import Dict, List, Set, Optional
from datetime import datetime
import sys

# 親ディレクトリをパスに追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# targets.json初期化
TARGETS_FILE = ROOT / "auto" / "targets.json"
if not TARGETS_FILE.exists():
    TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TARGETS_FILE.write_text('{"urls": [], "updated_at": null}', encoding="utf-8")

from auto.live_detector import LiveDetector
from facade import TwitCastingRecorder

# パス定義
LOGS_DIR = ROOT / "logs"
HEARTBEAT_FILE = LOGS_DIR / "heartbeat.json"
WRAPPER_LOG = LOGS_DIR / f"wrapper_{time.strftime('%Y%m%d')}.jsonl"


class MonitorEngine:
    """監視エンジン（重複防止・セマフォ制御）"""

    def __init__(self):
        # 設定読み込み
        self.config = self._load_config()
        self.enabled = self.config.get("enable_monitoring", False)
        self.monitor_cfg = self.config.get("monitor", {})

        # ディレクトリ準備
        try:
            LOGS_DIR.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass

        # 制御変数
        self.running = False
        self.in_flight: Dict[str, datetime] = {}
        self.semaphore = asyncio.Semaphore(self.monitor_cfg.get("max_concurrent_jobs", 2))
        self.tasks: Set[asyncio.Task] = set()  # タスク追跡

        # 統計
        self.stats = {
            "started_at": None,
            "total_checks": 0,
            "total_recordings": 0,
            "failed_recordings": 0,
            "error_403_count": 0,
            "last_error_code": None,
            "last_error_at": None
        }

        # 録画インスタンス（遅延初期化）
        self.recorder: Optional[TwitCastingRecorder] = None

    def _load_config(self) -> Dict:
        """config.json読み込み"""
        config_path = ROOT / "config.json"
        try:
            return json.loads(config_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[ERROR] Config load failed: {e}")
            return {}

    def _load_targets(self) -> List[str]:
        """targets.json読み込み"""
        if not TARGETS_FILE.exists():
            return []
        try:
            data = json.loads(TARGETS_FILE.read_text(encoding="utf-8"))
            urls = data.get("urls", [])
            return [str(u) for u in urls if isinstance(u, str)]
        except Exception as e:
            self._log_event("error", f"targets.json load failed: {e}")
            return []

    def _save_heartbeat(self):
        """heartbeat.json更新（atomic write）"""
        data = {
            "phase": "monitoring" if self.running else "stopped",
            "active_jobs": len(self.in_flight),
            "active_urls": list(self.in_flight.keys()),
            "stats": self.stats,
            "updated_at": datetime.now().isoformat(),
            "disk_free_gb": self._get_disk_free_gb()
        }
        try:
            LOGS_DIR.mkdir(parents=True, exist_ok=True)
            tmp_path = HEARTBEAT_FILE.with_suffix(".tmp")
            tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp_path.replace(HEARTBEAT_FILE)
        except Exception as e:
            print(f"[WARN] Heartbeat save failed: {e}")

    def _get_disk_free_gb(self) -> float:
        """ディスク空き容量（GB）"""
        try:
            stat = shutil.disk_usage(ROOT)
            return round(stat.free / (1024 ** 3), 2)
        except Exception:
            return 0.0

    def _log_event(self, event_type: str, detail: str = ""):
        """イベントログ（wrapper_*.jsonl）"""
        entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "detail": detail
        }
        try:
            LOGS_DIR.mkdir(parents=True, exist_ok=True)
            with open(WRAPPER_LOG, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _normalize_url(self, url: str) -> str:
        """URL正規化（統一ルール：c:/g:/ig:を含める）"""
        u = str(url).strip()
        if u.startswith(("http://", "https://")):
            return u
        if u.startswith(("c:", "g:", "ig:")):
            return f"https://twitcasting.tv/{u}"
        return f"https://twitcasting.tv/{u}"

    async def watch_and_record(self):
        """メインループ（ポーリング）"""
        if not self.enabled:
            print("[MONITOR] Monitoring disabled in config")
            return

        self.running = True
        self.stats["started_at"] = datetime.now().isoformat()
        poll_interval = int(self.monitor_cfg.get("poll_interval_sec", 30))
        disk_min_gb = float(self.monitor_cfg.get("disk_space_min_gb", 1))

        print(f"[MONITOR] Starting monitor (poll={poll_interval}s)")
        self._log_event("monitor_start", f"poll_interval={poll_interval}")

        # 録画インスタンス初期化
        self.recorder = TwitCastingRecorder()

        try:
            async with LiveDetector() as detector:
                while self.running:
                    self._save_heartbeat()

                    targets = self._load_targets()
                    for raw in targets:
                        if not self.running:
                            break

                        normalized = self._normalize_url(raw)

                        # 既に録画中ならスキップ
                        if normalized in self.in_flight:
                            continue

                        # 配信状態チェック
                        self.stats["total_checks"] += 1
                        status = await detector.check_live_status(normalized)
                        if not status.get("is_live"):
                            continue

                        # 容量チェック
                        if self._get_disk_free_gb() < disk_min_gb:
                            self._log_event("skip", f"Insufficient disk space for {normalized}")
                            continue

                        # スケジュール前にin_flight登録（重複防止）
                        self.in_flight[normalized] = datetime.now()

                        # 録画開始タスク作成
                        task = asyncio.create_task(self._start_recording(normalized, status))
                        self.tasks.add(task)
                        task.add_done_callback(self.tasks.discard)

                    await asyncio.sleep(poll_interval)

        except asyncio.CancelledError:
            print("[MONITOR] Cancelled")
        except Exception as e:
            print(f"[MONITOR] Error: {e}")
            self._log_event("error", str(e))
        finally:
            self.running = False
            self._save_heartbeat()

            # タスク終了待機
            if self.tasks:
                await asyncio.gather(*list(self.tasks), return_exceptions=True)

            # 録画インスタンスクリーンアップ
            if self.recorder:
                try:
                    await self.recorder.close(keep_chrome=True)
                except Exception:
                    pass

    async def _start_recording(self, url: str, status: Dict):
        """録画開始（エラー処理含む）"""
        try:
            async with self.semaphore:
                self.stats["total_recordings"] += 1

                self._log_event("recording_start", f"{url} - {status.get('title', '')}")
                print(f"[RECORD] Starting: {url}")

                # 録画実行
                result = await self.recorder.record(url)

                if result.get("success"):
                    self._log_event("recording_complete", f"{url} - {result.get('output_base', '')}")
                    print(f"[RECORD] Complete: {url}")
                else:
                    self.stats["failed_recordings"] += 1
                    error = result.get("error", "unknown")

                    if "403" in str(error):
                        self.stats["error_403_count"] += 1
                        self.stats["last_error_code"] = "403"
                    elif "M3U8_NOT_FOUND" in str(error):
                        self.stats["last_error_code"] = "NO_M3U8"
                    else:
                        self.stats["last_error_code"] = "UNKNOWN"

                    self.stats["last_error_at"] = datetime.now().isoformat()
                    self._log_event("recording_failed", f"{url} - {error}")
                    print(f"[RECORD] Failed: {url} - {error}")

                    # リトライ待機
                    if self.monitor_cfg.get("retry_on_error", True):
                        await asyncio.sleep(int(self.monitor_cfg.get("retry_delay_sec", 60)))
        except asyncio.CancelledError:
            self._log_event("recording_cancelled", url)
            print(f"[RECORD] Cancelled: {url}")
            raise
        except Exception as e:
            self._log_event("recording_error", f"{url} - {e}")
            print(f"[RECORD] Exception: {url} - {e}")
        finally:
            # 録画中マーク解除
            self.in_flight.pop(url, None)
            self._save_heartbeat()

    async def stop(self):
        """停止処理（タスク確実にキャンセル）"""
        print("[MONITOR] Stopping...")
        self.running = False

        # タスクをキャンセル
        for t in list(self.tasks):
            t.cancel()
        if self.tasks:
            await asyncio.gather(*list(self.tasks), return_exceptions=True)
        self.tasks.clear()

        # in_flightクリア
        self.in_flight.clear()
        self._save_heartbeat()

        # 録画クローズ
        if self.recorder:
            try:
                await self.recorder.close(keep_chrome=True)
            except Exception:
                pass


# テスト用
async def test():
    print("=== Monitor Engine Test ===")
    engine = MonitorEngine()
    engine.enabled = True  # テスト時は強制有効化
    task = asyncio.create_task(engine.watch_and_record())
    await asyncio.sleep(5)
    await engine.stop()
    await asyncio.sleep(1)
    print("Test complete")


if __name__ == "__main__":
    asyncio.run(test())