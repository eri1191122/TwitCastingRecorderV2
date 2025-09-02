#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor CLI for TwitCasting Recorder
コマンドライン制御
重大バグ修正版
- URL正規化統一
- ディレクトリ作成保証
- config.json読み込みエラー処理
"""
import asyncio
import json
import sys
import signal
from pathlib import Path
from typing import List
import argparse
import tempfile

# Windows EventLoop設定（最初に実行）
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 親ディレクトリをパスに追加
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# targets.json初期化
TARGETS_FILE = ROOT / "auto" / "targets.json"
if not TARGETS_FILE.exists():
    TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TARGETS_FILE.write_text('{"urls": [], "updated_at": null}', encoding="utf-8")

from auto.monitor_engine import MonitorEngine
from auto.live_detector import LiveDetector

# パス定義
HEARTBEAT_FILE = ROOT / "logs" / "heartbeat.json"

class MonitorCLI:
    """CLIコントローラー"""
    
    def __init__(self):
        self.engine = None
        self.task = None
        
    def _normalize_url(self, url: str) -> str:
        """URL正規化（Engine/Detectorと統一）"""
        u = str(url).strip()
        if u.startswith(("c:", "g:", "ig:")):
            return f"https://twitcasting.tv/{u}"  # 接頭辞を残す
        if u.startswith(("http://", "https://")):
            return u
        return f"https://twitcasting.tv/{u}"
        
    def _load_targets(self) -> dict:
        """targets.json読み込み"""
        if not TARGETS_FILE.exists():
            return {"urls": [], "updated_at": None}
        try:
            return json.loads(TARGETS_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {"urls": [], "updated_at": None}
            
    def _save_targets(self, data: dict):
        """targets.json保存（atomic write）"""
        # 親ディレクトリ作成
        TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # 更新日時追加
        from datetime import datetime
        data["updated_at"] = datetime.now().isoformat()
        
        # atomic write
        try:
            temp_file = TARGETS_FILE.with_suffix(".tmp")
            temp_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            temp_file.replace(TARGETS_FILE)
            print(f"[CLI] Saved {len(data['urls'])} targets")
        except Exception as e:
            print(f"[ERROR] Save failed: {e}")
            
    def add_url(self, url: str):
        """URL追加（正規化して保存）"""
        data = self._load_targets()
        normalized = self._normalize_url(url)
        
        # 重複チェック
        if normalized in data["urls"]:
            print(f"[CLI] Already exists: {url}")
            return
            
        data["urls"].append(normalized)  # 正規化したものを保存
        self._save_targets(data)
        print(f"[CLI] Added: {normalized}")
        
    def remove_url(self, url: str):
        """URL削除（正規化して削除）"""
        data = self._load_targets()
        normalized = self._normalize_url(url)
        
        if normalized in data["urls"]:
            data["urls"].remove(normalized)
            self._save_targets(data)
            print(f"[CLI] Removed: {normalized}")
        else:
            print(f"[CLI] Not found: {url}")
            
    def list_urls(self):
        """URL一覧表示"""
        data = self._load_targets()
        
        print(f"\n=== Monitoring Targets ({len(data['urls'])}) ===")
        for i, url in enumerate(data["urls"], 1):
            print(f"{i:2}. {url}")
            
        if data.get("updated_at"):
            print(f"\nLast updated: {data['updated_at']}")
            
        # heartbeat情報も表示
        if HEARTBEAT_FILE.exists():
            try:
                hb = json.loads(HEARTBEAT_FILE.read_text(encoding="utf-8"))
                print(f"\n=== Monitor Status ===")
                print(f"Phase: {hb.get('phase', 'unknown')}")
                print(f"Active jobs: {hb.get('active_jobs', 0)}")
                if hb.get("active_urls"):
                    print(f"Recording: {', '.join(hb['active_urls'])}")
                print(f"Disk free: {hb.get('disk_free_gb', 0)}GB")
            except Exception:
                pass
                
    async def check_url(self, url: str):
        """URL状態チェック"""
        print(f"[CLI] Checking: {url}")
        
        async with LiveDetector() as detector:
            result = await detector.check_live_status(url)
            
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
    async def start_monitoring(self):
        """監視開始（Ctrl+C対応）"""
        print("[CLI] Starting monitor...")
        
        # シグナルハンドラー設定
        def signal_handler(sig, frame):
            print("\n[CLI] Stopping (Ctrl+C)...")
            if self.task:
                self.task.cancel()
                
        signal.signal(signal.SIGINT, signal_handler)
        
        # エンジン起動
        self.engine = MonitorEngine()
        
        try:
            self.task = asyncio.create_task(self.engine.watch_and_record())
            await self.task
        except asyncio.CancelledError:
            print("[CLI] Cancelled")
        except Exception as e:
            print(f"[ERROR] {e}")
        finally:
            if self.engine:
                await self.engine.stop()
            print("[CLI] Stopped")

async def main():
    parser = argparse.ArgumentParser(
        description="TwitCasting Monitor CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --add https://twitcasting.tv/user_id
  %(prog)s --add c:user_id
  %(prog)s --add g:group_id
  %(prog)s --add ig:item_id
  %(prog)s --remove user_id
  %(prog)s --list
  %(prog)s --check https://twitcasting.tv/user_id
  %(prog)s --start
        """
    )
    
    parser.add_argument("--add", metavar="URL", help="Add monitoring target")
    parser.add_argument("--remove", metavar="URL", help="Remove target")
    parser.add_argument("--list", action="store_true", help="List targets")
    parser.add_argument("--check", metavar="URL", help="Check live status")
    parser.add_argument("--start", action="store_true", help="Start monitoring")
    parser.add_argument("--clear", action="store_true", help="Clear all targets")
    
    args = parser.parse_args()
    cli = MonitorCLI()
    
    # コマンド実行
    if args.add:
        cli.add_url(args.add)
    elif args.remove:
        cli.remove_url(args.remove)
    elif args.list:
        cli.list_urls()
    elif args.clear:
        cli._save_targets({"urls": []})
        print("[CLI] All targets cleared")
    elif args.check:
        await cli.check_url(args.check)
    elif args.start:
        # 設定確認（エラー処理追加）
        config_path = ROOT / "config.json"
        try:
            config = json.loads(config_path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"[ERROR] Failed to read config.json: {e}")
            return
            
        if not config.get("enable_monitoring"):
            print("[ERROR] Monitoring disabled in config.json")
            print("Set 'enable_monitoring': true to enable")
            return
            
        await cli.start_monitoring()
    else:
        parser.print_help()

if __name__ == "__main__":
    asyncio.run(main())