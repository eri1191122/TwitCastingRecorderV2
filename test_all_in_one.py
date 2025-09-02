#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
統合テスト - 単一プロセスでログイン→録画を実行
修正版：Windows NotImplementedError対策済み
"""
import asyncio
import sys
from pathlib import Path

# パス追加
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / "auto"))

async def main():
    """メイン処理"""
    print("=== 統合テスト開始 (修正版) ===")
    
    try:
        # インポート
        from facade import TwitCastingRecorder
        from auto.recorder_wrapper import RecorderWrapper
        
        # 1. ログイン状態確認
        print("\n[Step 1] ログイン状態確認中...")
        rec = TwitCastingRecorder()
        status = await rec.test_login_status()
        print(f"現在のログイン状態: {status}")
        
        # 2. 必要ならログイン
        if status != "strong":
            print("\n[Step 2] ログインが必要です")
            print("ブラウザが開きます。手動でログインしてください...")
            success = await rec.setup_login()
            if not success:
                print("❌ ログイン失敗。終了します")
                return
            print("✅ ログイン成功！")
        else:
            print("✅ 既にログイン済み")
        
        # 3. 録画テスト
        print("\n[Step 3] 録画テスト開始（10秒間）...")
        result = await RecorderWrapper.start_record(
            "g:117191215409354941008",  # テスト用URL
            duration=10,
            force_login_check=False  # 既にログイン確認済み
        )
        
        # 4. 結果表示
        print("\n[結果]")
        if result.get("ok"):
            print(f"✅ 録画成功！")
            print(f"   出力ファイル: {result.get('output_files', [])}")
            print(f"   経過時間: {result.get('elapsed_sec', 0):.1f}秒")
        else:
            print(f"❌ 録画失敗")
            print(f"   理由: {result.get('reason', 'unknown')}")
            if result.get('error'):
                print(f"   エラー: {result.get('error')}")
        
    except ImportError as e:
        print(f"\n❌ インポートエラー: {e}")
        print("必要なファイルが見つかりません:")
        print("  - facade.py")
        print("  - auto/recorder_wrapper.py")
        print("  - tc_recorder_core.py")
        print("  - core/chrome_singleton.py または chrome_singleton.py")
        
    except Exception as e:
        print(f"\n❌ 予期しないエラー: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 5. クリーンアップ
        print("\n[Step 4] クリーンアップ中...")
        try:
            await RecorderWrapper.shutdown()
            print("✅ シャットダウン完了")
        except Exception as e:
            print(f"⚠️ シャットダウンエラー: {e}")
        
        print("\n=== テスト完了 ===")

if __name__ == "__main__":
    # Windows環境でもProactorEventLoopがデフォルトなので特別な設定不要
    # Python 3.8以降はProactorEventLoopがWindowsのデフォルト
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⚠️ ユーザーによる中断")
    except Exception as e:
        print(f"\n❌ 実行エラー: {e}")
        import traceback
        traceback.print_exc()