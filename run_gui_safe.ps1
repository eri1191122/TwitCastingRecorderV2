# ==== TwitCasting Recorder GUI セーフ起動 (PowerShell版) ====
# Bash構文は禁止。python -c に統一して一発起動＆診断ログ出力。

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [Text.UTF8Encoding]::UTF8

# 0) スクリプトのある場所へ
Set-Location -LiteralPath (Split-Path -Parent $MyInvocation.MyCommand.Path)

# 1) 必要ディレクトリ
$logs = ".\logs"; $recs = ".\recordings"; $auth = ".\.auth\playwright"
New-Item -ItemType Directory -Force -Path $logs,$recs,$auth | Out-Null
$log = Join-Path $logs "gui_start.log"
"=== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') START ===" | Out-File -Encoding UTF8 $log

# 2) 依存チェック（PowerShellネイティブ）
function Require-Cli($cmd, $help) {
  if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
    Write-Host "コマンドが見つかりません: $cmd" -ForegroundColor Red
    Write-Host $help -ForegroundColor Yellow
    Pause; exit 1
  }
}

Require-Cli "python" "Pythonをインストールしてパスを通してください。"
"Python: $(python -V)" | Tee-Object -Append $log

# 3) playwright の有無判定（python -c でimport）
$playwrightOK = $false
try {
  python -c "import importlib; import sys; importlib.import_module('playwright'); print('playwright ok')" 2>$null | Tee-Object -Append $log
  $playwrightOK = $true
} catch { $playwrightOK = $false }

if (-not $playwrightOK) {
  Write-Host "playwright を導入します..." -ForegroundColor Yellow
  cmd /c "pip install --upgrade pip && pip install playwright" | Tee-Object -Append $log
}

# 4) Chromium の導入（既導入なら何もしない） ※python -m で実行
try {
  python -m playwright --version 2>$null | Tee-Object -Append $log
  python -m playwright install chromium | Tee-Object -Append $log
} catch {
  Write-Host "playwright のインストールに失敗しました" -ForegroundColor Red
  Pause; exit 1
}

# 5) 既存のChromeロック“表示だけ”
$lockNames = @("SingletonLock","SingletonCookie","SingletonSocket","DevToolsActivePort")
$locks = Get-ChildItem $auth -Recurse -ErrorAction SilentlyContinue | Where-Object { $lockNames -contains $_.Name }
if ($locks.Count -gt 0) {
  Write-Host "※ プロファイルにロック痕跡があります（起動失敗の原因になり得ます）" -ForegroundColor Yellow
  $locks | ForEach-Object { "LOCK: $($_.FullName)" | Tee-Object -Append $log }
}

# 6) GUIファイル健全性（CLIで上書きされてないか）
$gui = ".\tc_recorder_gui.py"
if (-not (Test-Path $gui)) {
  Write-Host "tc_recorder_gui.py が見つかりません。" -ForegroundColor Red
  Pause; exit 1
}
$head = (Get-Content $gui -TotalCount 12 -Encoding UTF8) -join "`n"
if ($head -match "Recorder Core" -and $head -notmatch "Recorder GUI") {
  $bk = ".\tc_recorder_gui.broken_$(Get-Date -UFormat %Y%m%d_%H%M%S).py"
  Copy-Item $gui $bk -Force
  Write-Host "⚠ GUIがCLIで上書きの疑い → $bk に退避しました。正しいGUIで作り直してください。" -ForegroundColor Yellow
  "GUI looked broken (Core detected). Backed up to $bk" | Tee-Object -Append $log
  Pause; exit 1
}

# 7) facade の存在
if (-not (Test-Path ".\core\facade.py") -and -not (Test-Path ".\facade.py")) {
  Write-Host "facade.py が見つかりません（core\facade.py でも可）。" -ForegroundColor Red
  Pause; exit 1
}

# 8) 起動（コンソールを閉じない）
$env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
Write-Host "GUI起動中..." -ForegroundColor Green
"Launching GUI..." | Tee-Object -Append $log

try {
  # ここを NoExit 相当で起動ログ表示
  python .\tc_recorder_gui.py 2>&1 | Tee-Object -Append $log
} catch {
  Write-Host "GUI起動に失敗: $_" -ForegroundColor Red
} finally {
  "=== $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') END ===" | Out-File -Append -Encoding UTF8 $log
  Write-Host "ログ: $log" -ForegroundColor DarkGray
  Pause
}
