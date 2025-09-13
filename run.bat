@echo off
setlocal enabledelayedexpansion

REM ================================================================
REM  TwitCastingRecorderV2 ランチャー（.venv 自動利用 / 文字化け対策）
REM  - メニュー実行 / 引数実行 / ドラッグ&ドロップ対応
REM  - 既存機能を削らず表示を安定化（UTF-8固定→終了時復帰）
REM ================================================================

REM ---- Console code page: remember & switch to UTF-8 to avoid mojibake ----
for /f "tokens=2 delims=: " %%A in ('chcp') do set "_PREV_CP=%%A"
chcp 65001 >nul

REM ---- Project root / Python path (.venv) ----
set "ROOT=%~dp0"
REM 末尾バックスラッシュ除去（見栄えだけ）
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "PY=%ROOT%\.venv\Scripts\python.exe"

REM ---- venv existence check ----
if not exist "%PY%" (
  echo [ERROR] .venv が見つかりません: "%PY%"
  echo.
  echo 初回セットアップ手順（PowerShell 推奨）:
  echo   py -3.13 -m venv .venv
  echo   .\.venv\Scripts\Activate.ps1
  echo   python -m pip install --upgrade pip
  echo   pip install playwright streamlink yt-dlp
  echo   playwright install chromium
  echo.
  echo セットアップ後に本ランチャーを再実行してください。
  goto :restore_cp_and_end
)

REM ---- If arguments are given or files are dropped onto this .bat, run directly ----
if not "%~1"=="" (
  call :run_args %*
  goto :restore_cp_and_end
)

REM =========================
REM メニュー表示
REM =========================
:menu
cls
echo ==============================================
echo  TwitCasting Recorder - ランチャー（UTF-8）
echo  .venv Python: "%PY%"
echo ==============================================
echo   [1] do_login.py        ・・・ ログインウィザード起動（手動ログイン）
echo   [2] monitor_gui.py     ・・・ GUI 起動（Login Check → Start Monitor）
echo   [3] auto\monitor_cli.py・・・ CLI 監視（targets.json 使用）
echo   [4] auto\monitor_engine.py・・・ 監視エンジン単体起動
echo   [5] auto\recorder_wrapper.py・・・ ラッパ単体テスト（必要に応じて引数追加）
echo   [6] 任意の .py を対話指定して実行
echo   [D] このバッチに .py をドラッグ&ドロップでも実行可（引数もOK）
echo   [Q] 終了
echo ----------------------------------------------
set /p SEL="番号を入力して Enter: "

if /I "%SEL%"=="1" goto run_do_login
if /I "%SEL%"=="2" goto run_gui
if /I "%SEL%"=="3" goto run_cli
if /I "%SEL%"=="4" goto run_engine
if /I "%SEL%"=="5" goto run_wrapper
if /I "%SEL%"=="6" goto run_pick
if /I "%SEL%"=="Q" goto restore_cp_and_end

echo.
echo 入力が不正です。やり直してください。
pause
goto :menu

REM =========================
REM 個別ターゲット
REM =========================
:run_do_login
call :run_one "do_login.py"
goto :menu

:run_gui
call :run_one "monitor_gui.py"
goto :menu

:run_cli
call :run_one "auto\monitor_cli.py"
goto :menu

:run_engine
call :run_one "auto\monitor_engine.py"
goto :menu

:run_wrapper
call :run_one "auto\recorder_wrapper.py"
goto :menu

:run_pick
set "PYPATH="
echo 実行したい .py のパスを入力するか、空Enterでメニューに戻ります。
set /p PYPATH="> "
if "%PYPATH%"=="" goto :menu
if not exist "%PYPATH%" (
  echo [ERROR] ファイルが見つかりません: "%PYPATH%"
  pause
  goto :menu
)
call :run_one "%PYPATH%"
goto :menu

REM =========================
REM 共通実行ルーチン
REM =========================
:run_one
set "_TARGET=%~1"
pushd "%ROOT%"
echo ----------------------------------------------
echo 実行: "%PY%" "%_TARGET%"
echo ----------------------------------------------
"%PY%" "%_TARGET%"
set "ERR=%ERRORLEVEL%"
popd
echo.
if "%ERR%"=="0" (
  echo [INFO] 終了コード 0（成功）
) else (
  echo [WARN] 終了コード %ERR%
)
echo.
pause
exit /b

REM =========================
REM 引数／D&D 実行
REM =========================
:run_args
pushd "%ROOT%"
echo ----------------------------------------------
echo 実行: "%PY%" %*
echo ----------------------------------------------
"%PY%" %*
set "ERR=%ERRORLEVEL%"
popd
echo.
if "%ERR%"=="0" (
  echo [INFO] 終了コード 0（成功）
) else (
  echo [WARN] 終了コード %ERR%
)
echo.
pause
exit /b

REM =========================
REM 終了前にコードページ復帰
REM =========================
:restore_cp_and_end
if defined _PREV_CP chcp !_PREV_CP! >nul
endlocal
exit /b
