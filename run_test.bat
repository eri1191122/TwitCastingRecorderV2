@echo off
echo === TwitCasting Recorder Test ===
echo.

echo [1/2] ログイン実行中...
python do_login.py
echo.

timeout /t 3 /nobreak > nul

echo [2/2] 録画テスト実行中...
cd auto
python recorder_wrapper.py
cd ..

echo.
echo === テスト完了 ===
pause