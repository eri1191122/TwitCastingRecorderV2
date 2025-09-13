#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor CLI for TwitCasting Recorder
監視機能のCLIインターフェース（統合版）
- ProactorEventLoop対応
- AUTH_REQUIRED明確化
- ログイン誘導実装
"""
import asyncio
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# Windows EventLoop設定（Proactor必須・最優先）
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# パス設定
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from auto.live_detector import LiveDetector
from auto.monitor_engine import MonitorEngine

# ファイルパス定義
TARGETS_FILE = ROOT / "auto" / "targets.json"
CONFIG_FILE = ROOT / "config.json"


class MonitorCLI:
    """監視CLIクラス"""
    
    def __init__(self):
        self.detector = LiveDetector()
        self.engine = None
        
    def load_targets(self):
        """監視対象リスト読み込み（BOM対応）"""
        if not TARGETS_FILE.exists():
            return []
        
        try:
            with open(TARGETS_FILE, "r", encoding="utf-8-sig") as f:
                data = json.load(f)
                return data.get("urls", [])
        except Exception as e:
            print(f"[ERROR] Failed to load targets: {e}")
            return []
    
    def save_targets(self, urls):
        """監視対象リスト保存（atomic write）"""
        data = {
            "urls": urls,
            "updated_at": datetime.now().isoformat()
        }
        
        try:
            TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
            # atomic write
            temp_file = TARGETS_FILE.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            temp_file.replace(TARGETS_FILE)
            return True
        except Exception as e:
            print(f"[ERROR] Failed to save targets: {e}")
            return False
    
    async def list_targets(self):
        """監視対象一覧表示"""
        urls = self.load_targets()
        
        if not urls:
            print("No monitoring targets configured.")
            return
        
        print(f"=== Monitoring Targets ({len(urls)}) ===")
        for url in urls:
            display_id = url.replace("https://twitcasting.tv/", "")
            print(f"  • {display_id}")
    
    async def add_target(self, target):
        """監視対象追加"""
        # URL正規化
        if not target.startswith("http"):
            target = f"https://twitcasting.tv/{target}"
        
        # c:プレフィックス削除
        if "twitcasting.tv/c:" in target:
            target = target.replace("/c:", "/")
        
        urls = self.load_targets()
        
        if target in urls:
            print(f"[INFO] Already exists: {target}")
            return
        
        urls.append(target)
        
        if self.save_targets(urls):
            print(f"[SUCCESS] Added: {target}")
        else:
            print(f"[ERROR] Failed to add: {target}")
    
    async def remove_target(self, target):
        """監視対象削除"""
        # URL正規化
        if not target.startswith("http"):
            target = f"https://twitcasting.tv/{target}"
        
        urls = self.load_targets()
        
        if target not in urls:
            print(f"[INFO] Not found: {target}")
            return
        
        urls.remove(target)
        
        if self.save_targets(urls):
            print(f"[SUCCESS] Removed: {target}")
        else:
            print(f"[ERROR] Failed to remove: {target}")
    
    async def check_target(self, target):
        """
        個別配信チェック（AUTH_REQUIRED明確化）
        """
        # URL正規化
        if not target.startswith("http"):
            target = f"https://twitcasting.tv/{target}"
        
        print(f"[CLI] Checking: {target}")
        
        # 互換I/F（check_live）を必ず使用
        result = await self.detector.check_live(target)
        
        # 結果表示（AUTH_REQUIREDを最優先で判定）
        if result.get("is_live"):
            print(f"🔴 LIVE: {result.get('movie_id', 'unknown')}")
            print(f"   Detail: {result.get('detail', '')}")
        else:
            reason = result.get("reason", "UNKNOWN")
            if reason == "AUTH_REQUIRED":
                print(f"🔒 AUTH_REQUIRED: {result.get('detail', 'ログインが必要')}")
                print("   → メン限/グル限の可能性があります")
                print("   → 以下のコマンドでログインしてください：")
                print("     python do_login.py")
                print("   → ログイン後に再度このコマンドを実行してください")
            elif reason == "NOT_LIVE":
                print(f"⚫ OFFLINE: 配信していません")
                print(f"   Detail: {result.get('detail', '')}")
            elif reason in ["NOT_FOUND", "USER_NOT_FOUND", "PAGE_NOT_FOUND"]:
                print(f"❌ NOT_FOUND: {result.get('detail', 'ページ/ユーザーが見つかりません')}")
            else:
                print(f"⚠️ {reason}: {result.get('detail', '')}")
        
        # JSON出力（reason握りつぶさない）
        print("\n[JSON Result]")
        print(json.dumps(result, ensure_ascii=False, indent=2))
        
        return result
    
    async def start_monitoring(self):
        """監視開始"""
        print("[CLI] Starting monitor...")
        
        # 設定読み込み（BOM対応）
        config = {}
        if CONFIG_FILE.exists():
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8-sig") as f:
                    config = json.load(f)
            except Exception as e:
                print(f"[WARN] Config load error: {e}")
        
        # 監視有効チェック（新旧両対応）
        monitor_config = config.get("monitor", {})
        monitor_enabled = (
            monitor_config.get("enable", False) or 
            config.get("enable_monitoring", False)
        )
        
        if not monitor_enabled:
            print("[ERROR] Monitoring is disabled in config.json")
            print("       Set monitor.enable = true to enable")
            print("       (or enable_monitoring = true for old version)")
            return
        
        # エンジン起動
        self.engine = MonitorEngine(config)
        
        try:
            await self.engine.start()
        except KeyboardInterrupt:
            print("\n[CLI] Stopping monitor...")
            if self.engine:
                await self.engine.stop()
            print("[CLI] Monitor stopped")
        except Exception as e:
            print(f"[ERROR] Monitor error: {e}")
            import traceback
            traceback.print_exc()
            if self.engine:
                await self.engine.stop()


async def main():
    parser = argparse.ArgumentParser(
        description="TwitCasting Monitor CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python auto/monitor_cli.py --list
  python auto/monitor_cli.py --add icchy8591
  python auto/monitor_cli.py --check nodasori2525
  python auto/monitor_cli.py --start
        """
    )
    parser.add_argument("--list", action="store_true", help="List monitoring targets")
    parser.add_argument("--add", metavar="URL", help="Add monitoring target")
    parser.add_argument("--remove", metavar="URL", help="Remove monitoring target")
    parser.add_argument("--check", metavar="URL", help="Check live status")
    parser.add_argument("--start", action="store_true", help="Start monitoring")
    
    args = parser.parse_args()
    
    cli = MonitorCLI()
    
    try:
        if args.list:
            await cli.list_targets()
        elif args.add:
            await cli.add_target(args.add)
        elif args.remove:
            await cli.remove_target(args.remove)
        elif args.check:
            await cli.check_target(args.check)
        elif args.start:
            await cli.start_monitoring()
        else:
            parser.print_help()
    except Exception as e:
        print(f"[FATAL] Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[CLI] Interrupted by user")
    except Exception as e:
        print(f"[FATAL] {e}")
        sys.exit(1)