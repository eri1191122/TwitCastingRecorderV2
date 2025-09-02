# TwitCasting Recorder v2.0

限定配信対応・Chrome起動最小化を実現した高効率録画システム

## 特徴

- ✅ **限定配信対応**: 年齢制限、グループ限定、メンバーシップ限定に対応
- ✅ **Chrome起動最小化**: 永続プロファイルによるセッション再利用
- ✅ **簡単GUI**: ボタンクリックで録画開始
- ✅ **自動化**: バックグラウンドでセッション維持
- ✅ **テスト機能**: ワンクリックで動作確認

## セットアップ

### 1. 必要なもの
- Python 3.8以上
- ffmpeg（yt-dlp用）

### 2. インストール
```bash
# Windowsの場合
setup.bat

# Mac/Linuxの場合
python -m venv venv
source venv/bin/activate  # Mac/Linux
pip install -r requirements.txt
playwright install chromium