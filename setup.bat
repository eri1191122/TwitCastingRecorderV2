@echo off
echo TwitCasting Recorder セットアップを開始します...

REM Python仮想環境作成
echo 仮想環境を作成中...
python -m venv venv
call venv\Scripts\activate

REM パッケージインストール
echo 必要なパッケージをインストール中...
pip install --upgrade pip
pip install -r requirements.txt

REM Playwrightブラウザインストール
echo Chromiumブラウザをインストール中...
playwright install chromium

REM ディレクトリ作成
echo ディレクトリを作成中...
mkdir logs 2>nul
mkdir recordings 2>nul
mkdir .auth\playwright 2>nul

echo.
echo セットアップが完了しました！
echo.
echo 使い方:
echo 1. run.bat をダブルクリックしてGUIを起動
echo 2. 初回は「テスト」タブから「ログインセットアップ」を実行
echo 3. 「録画」タブでURLを入力して録画開始
echo.
pause