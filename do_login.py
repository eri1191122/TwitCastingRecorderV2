import asyncio
from facade import TwitCastingRecorder

async def login():
    rec = TwitCastingRecorder()
    print("=" * 50)
    print("ブラウザが開きます")
    print("TwitCastingに手動でログインしてください")
    print("ログイン完了したら自動で終了します")
    print("=" * 50)
    
    result = await rec.setup_login()
    
    if result:
        print("✅ ログイン成功！Cookieが保存されました")
        print("今後30日間は自動でログイン状態が維持されます")
    else:
        print("❌ ログイン失敗...")
    
    await rec.close(keep_chrome=False)

if __name__ == "__main__":
    asyncio.run(login())