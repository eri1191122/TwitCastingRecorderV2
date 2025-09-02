#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TwitCasting Recorder Core v4.6 (最終修正版)
- v4.4の全機能を維持
- 【修正】_is_real_hls関数のドメイン判定厳格化
- 【修正】_capture_m3u8のインデント修正
- 【追加】page.offでリスナー解除
- 【追加】_pass_membership_gate（ゲート自動突破）
"""
from __future__ import annotations  # 最上段

# ROOT挿入ガード
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 以降は既存のimport
import asyncio
import json
import shutil
import time
import re
import uuid
import tempfile
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

# aiofiles は任意依存
try:
    import aiofiles
    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False
    print("[INFO] aiofiles not found, using synchronous I/O")

from playwright.async_api import BrowserContext, Page

# ===== パス =====
LOGS = ROOT / "logs"
RECORDINGS = ROOT / "recordings"
AUTH_DIR = ROOT / ".auth" / "playwright"
for d in (LOGS, RECORDINGS, AUTH_DIR):
    d.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = ROOT / "config.json"

# ===== 診断ログ（Core 用） =====
class CoreDiagnostics:
    """コア用診断ログ（ChromeDiagnostics と分離）"""
    @staticmethod
    def log(msg: str, level: str = "INFO") -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        log_path = LOGS / "core_diagnostic.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"{ts} [{level}] {msg}\n")
            print(f"[CORE-{level}] {msg}")
        except Exception as e:
            print(f"[CORE-LOG-ERROR] {e}")

# ===== chrome_singleton のインポート（core./直下の両対応） =====
try:
    from core.chrome_singleton import get_chrome_singleton  # type: ignore
    CoreDiagnostics.log("Imported chrome_singleton from core.chrome_singleton", "DEBUG")
except Exception as e1:
    try:
        from chrome_singleton import get_chrome_singleton  # type: ignore
        CoreDiagnostics.log("Imported chrome_singleton from chrome_singleton", "DEBUG")
    except Exception as e2:
        CoreDiagnostics.log(f"Failed to import chrome_singleton: core=({e1}), root=({e2})", "ERROR")
        raise RuntimeError(
            "chrome_singleton のインポートに失敗しました。ファイル配置（core/配下か直下か）を確認してください。"
        )

# ===== 【追加】本編HLS判定関数（ドメイン判定厳格化） =====
def _is_real_hls(url: str) -> bool:
    """
    TwitCastingの本編HLSだけを通す判定（プレビュー等は除外）
    ドメイン判定を厳格化
    
    Args:
        url: 検査するURL
        
    Returns:
        本編HLSならTrue、プレビュー等ならFalse
    """
    u = url.lower()
    
    # m3u8以外は即除外
    if ".m3u8" not in u:
        return False
    
    # 明確に除外するパターン（プレビュー・サムネ・広告・疑似プレイリスト）
    deny_patterns = (
        "preview", "thumbnail", "thumb", "ad/", "/ads/", "ad=",
        "preroll", "dash", "manifest", "test", "dummy", "sample",
        "error", "offline", "maintenance", "placeholder"
    )
    if any(pattern in u for pattern in deny_patterns):
        return False
    
    # 本編系の許可パターン（TwitCastingで実際に多いパターン）
    allow_patterns = (
        "/hls/", "/live/", "livehls", "tc.livehls", "/tc.hls/",
        "/streams/", "/media.m3u8", "/master.m3u8"
    )
    
    # ドメイン判定（厳格化：正規ドメインのみ）
    valid_domains = (
        ".twitcasting.tv/" in u or ".twitcasting.net/" in u or
        "://twitcasting.tv/" in u or "://twitcasting.net/" in u
    )
    
    # 両方の条件を満たす場合のみTrue
    has_allow_pattern = any(pattern in u for pattern in allow_patterns)
    return valid_domains and has_allow_pattern

# ===== 設定 =====
@dataclass
class Config:
    # 実行環境
    headless: bool = True
    ffmpeg_path: str = ""            # 例: "C:\\ffmpeg\\bin"
    ytdlp_path: str = "yt-dlp"       # PATH に無い場合はフルパス
    keep_alive_interval: int = 1800  # セッション点検間隔(秒)

    # 録画
    preferred_quality: str = "best"
    default_duration: int = 600
    test_duration: int = 10

    # デバッグ
    debug_mode: bool = False
    verbose_log: bool = False
    save_network_log: bool = False
    enable_group_gate_auto: bool = False  # ゲート自動突破

    # 認証
    auto_heal: bool = True
    m3u8_timeout: int = 45
    cookie_domain: str = ".twitcasting.tv"

    @classmethod
    def load(cls) -> "Config":
        if CONFIG_PATH.exists():
            try:
                data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
                base = asdict(cls())
                base.update(data)
                return cls(**base)
            except Exception as e:
                print(f"[WARN] Config load error: {e}")
        cfg = cls()
        cfg.save()
        return cfg

    def save(self) -> None:
        """安全な設定保存（アトミック操作）"""
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=CONFIG_PATH.parent, delete=False
            ) as tf:
                temp_path = Path(tf.name)
                json.dump(asdict(self), tf, ensure_ascii=False, indent=2)
            temp_path.replace(CONFIG_PATH)
        except Exception as e:
            print(f"[WARN] Config save error: {e}")
            try:
                if "temp_path" in locals():
                    temp_path.unlink(missing_ok=True)
            except Exception:
                pass

# ===== ユーティリティ =====
def _now() -> str:
    return time.strftime("%Y%m%d_%H%M%S")

def _extract_user_id(url: str) -> Optional[str]:
    m = re.search(r"twitcasting\.tv/([^/\?]+)", url)
    return m.group(1) if m else None

def _check_login_status(cookies: List[dict]) -> str:
    """strong/weak/none を返す（将来利用のため残置）"""
    names = {c.get("name", "") for c in cookies}
    primary = {"tc_ss", "_twitcasting_session", "tc_s"}
    secondary = {"tc_id", "tc_u"}
    if names & primary:
        return "strong"
    if names & secondary:
        return "weak"
    return "none"

async def _save_cookies_netscape(context: BrowserContext, path: Path, domain_filter: Optional[str] = None) -> None:
    cookies = await context.cookies()
    if domain_filter:
        df = domain_filter.lstrip(".")
        def _match(d: str) -> bool:
            d = (d or "").lstrip(".")
            return d == df or d.endswith("." + df)
        cookies = [c for c in cookies if _match(c.get("domain", ""))]

    lines = ["# Netscape HTTP Cookie File\n"]
    now_exp = int(time.time() + 86400 * 30)
    for c in cookies:
        domain = c.get("domain", "")
        include_sub = "TRUE" if domain.startswith(".") else "FALSE"
        pth = c.get("path", "/")
        secure = "TRUE" if c.get("secure") else "FALSE"
        exp = c.get("expires", None)
        expires = int(exp) if isinstance(exp, (int, float)) and exp > 0 else now_exp
        name = c.get("name", "")
        value = c.get("value", "")
        lines.append(f"{domain}\t{include_sub}\t{pth}\t{secure}\t{expires}\t{name}\t{value}\n")

    if HAS_AIOFILES:
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            await f.writelines(lines)
    else:
        path.write_text("".join(lines), encoding="utf-8")

def _ensure_dir(w: Path) -> None:
    w.mkdir(parents=True, exist_ok=True)

def _shell_which(cmd: str) -> Optional[str]:
    return shutil.which(cmd)

async def _graceful_terminate(proc: asyncio.subprocess.Process, soft_timeout: float = 10.0) -> None:
    """改善されたプロセス終了処理"""
    if proc is None:
        return
    # 1) terminate
    try:
        proc.terminate()
        await asyncio.wait_for(proc.wait(), timeout=soft_timeout)
        CoreDiagnostics.log("Process terminated gracefully", "DEBUG")
        return
    except (ProcessLookupError, asyncio.TimeoutError):
        pass
    except Exception as e:
        CoreDiagnostics.log(f"Terminate error: {e}", "WARN")
    # 2) kill
    try:
        proc.kill()
        await asyncio.wait_for(proc.wait(), timeout=5.0)
        CoreDiagnostics.log("Process killed", "WARN")
    except ProcessLookupError:
        pass
    except asyncio.TimeoutError:
        CoreDiagnostics.log("Process kill timeout", "ERROR")
    except Exception as e:
        CoreDiagnostics.log(f"Kill error: {e}", "ERROR")

def _compose_output_base(url: str) -> Path:
    user_id = _extract_user_id(url) or "unknown"
    # Windowsファイル名サニタイズ
    user_id = re.sub(r'[:<>"|?*/\\]', '_', user_id)
    return RECORDINGS / f"{_now()}_{user_id}_{uuid.uuid4().hex[:8]}"

def _check_error_in_tail(tail: List[str]) -> bool:
    """403エラーや 0 bytes をチェック"""
    error_patterns = ["403", "Forbidden", "0 bytes", "ERROR"]
    for line in tail[-50:]:
        for pattern in error_patterns:
            if pattern in line:
                return True
    return False

# ===== 自己診断 =====
def self_check(cfg: Config) -> Dict[str, Any]:
    ok = True
    problems = []

    ytdlp_path = cfg.ytdlp_path or "yt-dlp"
    if not _shell_which(ytdlp_path) and not Path(ytdlp_path).exists():
        ok = False
        problems.append(f"yt-dlp not found: {ytdlp_path}")

    if cfg.ffmpeg_path:
        ff = Path(cfg.ffmpeg_path)
        if ff.is_dir():
            ff_exe = ff / "ffmpeg.exe"
            if not (ff_exe.exists() or (ff / "ffmpeg").exists()):
                problems.append(f"ffmpeg not found in: {cfg.ffmpeg_path}")
        else:
            if not ff.exists():
                problems.append(f"ffmpeg path invalid: {cfg.ffmpeg_path}")

    try:
        _ensure_dir(RECORDINGS)
        (LOGS / "write_test.txt").write_text("ok", encoding="utf-8")
        (LOGS / "write_test.txt").unlink(missing_ok=True)
    except Exception as e:
        ok = False
        problems.append(f"logs/recordings write error: {e}")

    return {"ok": ok, "problems": problems}

# ===== 録画エンジン =====
class RecordingEngine:
    def __init__(self, chrome_singleton) -> None:
        self.chrome = chrome_singleton
        self.cfg = Config.load()
        self._netlog: List[Dict[str, Any]] = []

    async def _wait_for_player_ready(self, page: Page) -> None:
        try:
            await page.wait_for_selector("video, iframe[src*='player'], [class*='player'], #player", timeout=10000)
            await page.wait_for_timeout(1500)
        except Exception:
            pass

    async def _trigger_playback(self, page: Page) -> None:
        methods = [
            ("click", "video"),
            ("click", "[class*='player']"),
            ("click", "#player"),
            ("click", "[aria-label*='再生']"),
            ("click", "[aria-label*='Play']"),
            ("key", "Space"),
            ("js", """
                () => {
                    const v=document.querySelector('video');
                    if(v){ v.play().catch(()=>{}); return true; }
                    return false;
                }
            """),
        ]
        for t, x in methods:
            try:
                if t == "click":
                    await page.click(x, timeout=800)
                elif t == "key":
                    await page.keyboard.press(x)
                else:
                    await page.evaluate(x)
                await page.wait_for_timeout(300)
            except Exception:
                continue

    # ===== 【追加】ゲート自動突破メソッド =====
    async def _pass_membership_gate(self, page: Page) -> bool:
        """
        入場/同意ボタンを自動クリック（configがtrueの時だけ）
        
        Args:
            page: Playwrightのページオブジェクト
            
        Returns:
            クリック成功したらTrue
        """
        if not getattr(self.cfg, "enable_group_gate_auto", False):
            return False
        
        clicked = False
        candidates = [
            # テキストベース（日本語）
            "text=入場", "text=視聴する", "text=同意して進む", "text=続ける",
            "text=OK", "text=はい", "text=入室", "text=参加", "text=入る",
            # テキストベース（英語）
            "text=Enter", "text=Join", "text=Continue", "text=Watch", "text=Agree",
            # data属性
            "[data-test*='enter']", "[data-test*='join']", "[data-test*='gate']",
            "[data-test*='membership']",
            # aria属性
            "[aria-label*='入場']", "[aria-label*='視聴']", "[aria-label*='参加']",
            "[aria-label*='Enter']", "[aria-label*='Join']",
            # ボタン要素
            "button:has-text('入場')", "button:has-text('視聴')",
            "button:has-text('入室')", "button:has-text('参加')",
            "button:has-text('Enter')", "button:has-text('Join')", "button:has-text('Watch')",
            # リンク要素
            "a[href*='membershipjoinplans']", "a[href*='membership']", "a[href*='join']",
            # クラス名
            "[class*='enter-button']", "[class*='gate-button']",
            "[class*='join-button']", "[class*='membership-button']", "[class*='tc-button']",
        ]
        
        try:
            for sel in candidates:
                try:
                    if await page.is_visible(sel, timeout=500):
                        CoreDiagnostics.log(f"Gate button found: {sel}", "INFO")
                        await page.click(sel, timeout=1200)
                        await page.wait_for_timeout(800)
                        clicked = True
                        CoreDiagnostics.log(f"Gate button clicked: {sel}", "INFO")
                        break
                except Exception:
                    continue
        except Exception as e:
            CoreDiagnostics.log(f"Gate pass error: {e}", "DEBUG")
        
        return clicked

    # ===== 【修正】_capture_m3u8（インデント修正+リスナー解除） =====
    async def _capture_m3u8(self, page: Page, base_timeout: int = 20) -> Optional[str]:
        """
        本編HLSのみをキャプチャ（プレビュー除外版）
        インデント修正済み、リスナー解除追加
        
        Args:
            page: Playwrightページ
            base_timeout: 基本タイムアウト秒数
            
        Returns:
            本編のm3u8 URL or None
        """
        found = {"u": None}
        
        # レスポンスフック定義：本編HLSだけ拾う
        def on_resp(resp):
            try:
                u = resp.url
                if getattr(self.cfg, "save_network_log", False):
                    self._netlog.append({
                        "url": u, 
                        "status": resp.status,
                        "time": time.time(), 
                        "is_real": _is_real_hls(u)
                    })
                if not found["u"] and _is_real_hls(u):
                    found["u"] = u
                    CoreDiagnostics.log(f"Real HLS detected: {u[:100]}...", "INFO")
            except Exception:
                pass
        
        # 【重要】関数定義の外でリスナー登録
        page.on("response", on_resp)
        
        try:
            # Phase 1: プレイヤー準備
            CoreDiagnostics.log("Waiting for player ready...", "DEBUG")
            try:
                await self._wait_for_player_ready(page)
            except Exception:
                pass
            
            # Phase 2: ゲート突破試行
            gate_clicked = await self._pass_membership_gate(page)
            if gate_clicked:
                await page.wait_for_timeout(1500)
                try:
                    await self._wait_for_player_ready(page)
                except Exception:
                    pass
            
            # Phase 3: 再生トリガー
            CoreDiagnostics.log("Triggering playback...", "DEBUG")
            try:
                await self._trigger_playback(page)
            except Exception:
                pass
            
            # Phase 4: 本編HLS待機（段階的）
            short = max(5, base_timeout // 3)
            strategies = ["wait", "play_again", "reload"]
            
            for strategy in strategies:
                if found["u"]:
                    break
                
                try:
                    if strategy == "wait":
                        CoreDiagnostics.log(f"Waiting for real HLS... ({short}s)", "DEBUG")
                        
                    elif strategy == "play_again":
                        CoreDiagnostics.log("Retrying playback trigger", "DEBUG")
                        await self._trigger_playback(page)
                        await page.wait_for_timeout(500)
                        
                    elif strategy == "reload":
                        CoreDiagnostics.log("Reloading page for retry", "DEBUG")
                        await page.reload(wait_until="domcontentloaded", timeout=15000)
                        await page.wait_for_timeout(2000)
                        
                        # 再度全手順
                        try:
                            await self._wait_for_player_ready(page)
                        except Exception:
                            pass
                        
                        gate_clicked = await self._pass_membership_gate(page)
                        if gate_clicked:
                            await page.wait_for_timeout(1500)
                        
                        await self._trigger_playback(page)
                    
                    # 本編HLS検出待ち（_is_real_hlsを使う）
                    await page.wait_for_event(
                        "response",
                        predicate=lambda r: _is_real_hls(r.url),
                        timeout=short * 1000
                    )
                    
                except Exception:
                    CoreDiagnostics.log(f"Strategy '{strategy}' timeout/failed", "DEBUG")
                    continue
            
            # 結果処理
            if found["u"]:
                CoreDiagnostics.log(f"Captured real HLS: {found['u']}", "SUCCESS")
            else:
                CoreDiagnostics.log("No real HLS found after all strategies", "WARNING")
                
                # デバッグ用：キャプチャされたURLを表示
                if getattr(self.cfg, "debug_mode", False) and self._netlog:
                    print("[CORE-DEBUG] Last captured URLs:")
                    for log in self._netlog[-20:]:
                        if ".m3u8" in log.get("url", ""):
                            is_real = log.get("is_real", False)
                            status = "✓" if is_real else "✗"
                            print(f"  {status} {log['url'][:100]}")
            
        finally:
            # 【追加】リスナー解除（メモリリーク防止）
            try:
                page.remove_listener("response", on_resp)
                CoreDiagnostics.log("Response listener removed", "DEBUG")
            except Exception:
                pass
        
        return found["u"]

    async def _execute_ytdlp(
        self,
        m3u8: str,
        page: Page,
        out_tpl: str,
        cookies_path: Path,
        duration: Optional[int] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        """yt-dlp 実行（賢い画質判定つき）"""

        # UA（chrome_singleton 統一 UA → 取得不能なら page 側）
        ua = getattr(self.chrome, "get_unified_ua", lambda: None)() or "Mozilla/5.0"
        try:
            if ua == "Mozilla/5.0":
                ua = await page.evaluate("navigator.userAgent")
        except Exception:
            pass

        cmd = [
            (self.cfg.ytdlp_path or "yt-dlp"),
            m3u8,
            "--no-part",
            "--concurrent-fragments", "4",
            "-N", "4",
            "--retries", "20",
            "--fragment-retries", "20",
            "--retry-sleep", "3",
            "--add-header", f"Referer: {page.url}",
            "--add-header", "Origin: https://twitcasting.tv",
            "--cookies", str(cookies_path),
            "--no-check-certificate",
            "-o", out_tpl,
            "--user-agent", ua,
            "--remux-video", "mp4",
        ]

        # 画質設定（-f/-S を賢く自動判別）
        val = (self.cfg.preferred_quality or "").strip()
        if val:
            # 「フォーマット式」なら -f：[], +, / を含む or bestvideo/bestaudio を含む
            use_f = any(ch in val for ch in "[]+/") or "bestvideo" in val or "bestaudio" in val
            mode = "-f" if use_f else "-S"
            cmd.extend([mode, val])
            CoreDiagnostics.log(f"quality_mode={mode} value={val}", "INFO")
        else:
            cmd.extend(["-f", "best"])
            CoreDiagnostics.log("quality_mode=-f value=best (default)", "INFO")

        if self.cfg.ffmpeg_path:
            cmd.extend(["--ffmpeg-location", self.cfg.ffmpeg_path])
        if duration and duration > 0:
            cmd.extend(["--download-sections", f"*0-{duration}"])

        start_ts = time.time()
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(ROOT),
        )

        tail: List[str] = []

        async def _reader():
            nonlocal tail
            try:
                while True:
                    line = await proc.stdout.readline()
                    if not line:
                        break
                    s = line.decode(errors="ignore").rstrip()
                    tail.append(s)
                    if len(tail) > 200:
                        tail = tail[-120:]
            except Exception:
                pass

        reader_task = asyncio.create_task(_reader())

        if duration and duration > 0:
            try:
                await asyncio.wait_for(proc.wait(), timeout=duration + 60)
            except asyncio.TimeoutError:
                await _graceful_terminate(proc)
        else:
            await proc.wait()

        await reader_task

        rc = proc.returncode or 0
        elapsed = round(time.time() - start_ts, 1)

        return {
            "return_code": rc,
            "elapsed": elapsed,
            "tail": tail,
            "retry_count": retry_count
        }

    async def record(self, url: str, duration: Optional[int] = None) -> Dict[str, Any]:
        print(f"[INFO] Starting recording: {url}")
        if duration:
            print(f"[INFO] Duration: {duration}s")

        CoreDiagnostics.log("Recording started", "INFO")

        try:
            check = self_check(self.cfg)
            if not check["ok"]:
                return {"success": False, "error": "self_check_failed", "details": check["problems"]}

            # chrome_singleton 経由（record は基本ヘッドレス）
            ctx = await self.chrome.ensure_headless()
            if ctx is None:
                CoreDiagnostics.log("ensure_headless() returned None", "ERROR")
                return {"success": False, "error": "chrome_context_none",
                        "details": "chrome_singleton の ensure_headless() が失敗"}

            page: Optional[Page] = None
            out_base = _compose_output_base(url)
            out_tpl = str(out_base) + ".%(ext)s"

            try:
                page = await ctx.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await self._wait_for_player_ready(page)

                # JIT #1
                await _save_cookies_netscape(ctx, LOGS / f"cookies_enter_{_now()}.txt", self.cfg.cookie_domain)

                # m3u8 検出（修正版を使用）
                m3u8 = await self._capture_m3u8(page, base_timeout=self.cfg.m3u8_timeout)

                if not m3u8:
                    if self.cfg.save_network_log and self._netlog:
                        (LOGS / f"net_debug_{_now()}.json").write_text(
                            json.dumps(self._netlog, ensure_ascii=False, indent=2),
                            encoding="utf-8"
                        )
                    return {"success": False, "error": "M3U8_NOT_FOUND"}

                # JIT #2
                cookies_path = LOGS / f"cookies_m3u8_{_now()}.txt"
                await _save_cookies_netscape(ctx, cookies_path, self.cfg.cookie_domain)

                # yt-dlp（最初の試行）
                exec_result = await self._execute_ytdlp(
                    m3u8, page, out_tpl, cookies_path, duration, retry_count=0
                )

                rc = exec_result["return_code"]
                tail = exec_result["tail"]
                bad_format = any("Requested format is not available" in s for s in tail[-50:])

                # 403 / 0bytes / フォーマット不一致 → 1回だけ再試行
                if (rc != 0 or _check_error_in_tail(tail) or bad_format) and exec_result["retry_count"] == 0:
                    reason = "bad_format" if bad_format else "network_or_http"
                    CoreDiagnostics.log(f"Retry triggered (reason={reason})", "WARN")
                    print("[WARN] Download error detected, retrying with fresh cookies...")

                    # Cookie 再取得（JIT）
                    cookies_path_retry = LOGS / f"cookies_retry_{_now()}.txt"
                    await _save_cookies_netscape(ctx, cookies_path_retry, self.cfg.cookie_domain)

                    if bad_format:
                        # フォーマット不一致は強制的に -f best で再実行
                        CoreDiagnostics.log("Retry reason=bad_format -> force -f best", "WARN")
                        original = self.cfg.preferred_quality
                        try:
                            self.cfg.preferred_quality = "best"
                            exec_result = await self._execute_ytdlp(
                                m3u8, page, out_tpl, cookies_path_retry, duration, retry_count=1
                            )
                        finally:
                            self.cfg.preferred_quality = original
                    else:
                        # 403/0bytes 系は通常の再試行（JIT Cookie で再実行）
                        exec_result = await self._execute_ytdlp(
                            m3u8, page, out_tpl, cookies_path_retry, duration, retry_count=1
                        )

                    rc = exec_result["return_code"]
                    tail = exec_result["tail"]

                # 結果構築
                result = {
                    "success": (rc == 0),
                    "code": rc,
                    "elapsed": exec_result["elapsed"],
                    "m3u8": m3u8,
                    "output_base": str(out_base),
                    "output_files": [str(p) for p in RECORDINGS.glob(f"{out_base.name}.*")],
                    "tail": tail[-60:],
                    "retry_count": exec_result["retry_count"]
                }

                if self.cfg.debug_mode:
                    (LOGS / f"record_result_{_now()}.json").write_text(
                        json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8"
                    )

                print(f"[INFO] Recording complete: success={result['success']}, retries={result['retry_count']}")
                CoreDiagnostics.log(f"Recording complete: {result['success']}", "INFO")
                return result

            except asyncio.CancelledError:
                CoreDiagnostics.log("Recording cancelled", "WARN")
                return {"success": False, "error": "cancelled"}
            except Exception as e:
                print(f"[ERROR] Recording error: {e}")
                CoreDiagnostics.log(f"Recording error: {e}", "ERROR")
                return {"success": False, "error": f"exception: {e}"}
            finally:
                if page:
                    try:
                        await page.close()
                    except Exception:
                        pass
                if self.cfg.save_network_log and self._netlog:
                    try:
                        (LOGS / f"net_final_{_now()}.json").write_text(
                            json.dumps(self._netlog, ensure_ascii=False, indent=2),
                            encoding="utf-8"
                        )
                    except Exception:
                        pass
        finally:
            CoreDiagnostics.log("Recording ended", "INFO")

# ===== 外部公開ラッパ =====
class TwitCastingRecorder:
    def __init__(self) -> None:
        print(f"[DEBUG] TwitCastingRecorder init (Chrome Singleton version)")
        self.chrome = get_chrome_singleton()
        self.engine = RecordingEngine(self.chrome)
        self.is_recording = False
        self._initialized = False
        self.cfg = Config.load()
        self._record_lock = asyncio.Lock()  # 録画ロック
        self._init_lock = asyncio.Lock()    # 初期化ロック

    async def initialize(self) -> None:
        """初期化（スレッドセーフ＆設定反映）"""
        async with self._init_lock:
            if self._initialized:
                return
            ctx = None
            if self.cfg.headless:
                ctx = await self.chrome.ensure_headless()
            else:
                ctx = await self.chrome.ensure_visible()
            if ctx is None:
                CoreDiagnostics.log("chrome ensure_*() returned None (起動失敗)", "ERROR")
                raise RuntimeError("Chrome 起動に失敗しました。chrome_singleton のログと依存関係を確認してください。")
            self._initialized = True
            print("[DEBUG] TwitCastingRecorder initialized with chrome_singleton")
            CoreDiagnostics.log("TwitCastingRecorder initialized", "INFO")

    async def setup_login(self) -> bool:
        """ガイド付きログイン（可視ウィザード）"""
        try:
            status = await self.chrome.check_login_status()
            if status == "strong":
                print("[LOGIN] Already logged in!")
                return True
            print("[LOGIN] Starting guided login wizard...")
            ok = await self.chrome.guided_login_wizard()
            CoreDiagnostics.log(f"Login via wizard: {ok}", "INFO")
            return ok
        except Exception as e:
            print(f"[ERROR] Login setup error: {e}")
            CoreDiagnostics.log(f"Login setup error: {e}", "ERROR")
            return False

    async def test_login_status(self) -> str:
        """
        ログイン状態の軽量確認
        【修正】副作用ゼロ版、initialize()呼ばない
        """
        try:
            # 【修正】直接chrome_singletonの状態確認を呼ぶ（副作用なし）
            status = await self.chrome.check_login_status()
            print(f"[DEBUG] Login status: {status}")
            CoreDiagnostics.log(f"Login status check: {status}", "INFO")
            return status if status in ("strong", "weak", "none") else "none"
        except Exception as e:
            print(f"[ERROR] Login check error: {e}")
            CoreDiagnostics.log(f"Login check error: {e}", "ERROR")
            return "none"

    async def record(self, url: str, duration: Optional[int] = None) -> Dict[str, Any]:
        """録画実行（ロック保護）"""
        async with self._record_lock:
            if self.is_recording:
                CoreDiagnostics.log("Already recording", "WARN")
                return {"success": False, "error": "already_recording"}
            self.is_recording = True
            try:
                return await self.engine.record(url, duration)
            finally:
                self.is_recording = False

    async def test_record(self, url: str) -> Dict[str, Any]:
        """テスト録画（短時間）"""
        du = self.cfg.test_duration or 10
        print(f"[TEST] Starting test recording ({du} seconds)...")
        return await self.record(url, duration=du)

    async def close(self, keep_chrome: bool = False, **kwargs) -> None:
        """
        終了処理
        keep_chrome: True の場合、Chrome は閉じない（GUI 用）
        """
        print(f"[DEBUG] TwitCastingRecorder.close called (keep_chrome={keep_chrome})")
        CoreDiagnostics.log(f"Close called (keep_chrome={keep_chrome})", "INFO")
        if not keep_chrome:
            try:
                await self.chrome.close()
            finally:
                self._initialized = False

    # ===== 後方互換プロパティ =====
    @property
    def session(self):
        """後方互換：session プロパティ（廃止予定）"""
        class _DummySession:
            def __init__(self, recorder):
                self.cfg = recorder.cfg
                self.is_recording = recorder.is_recording
        return _DummySession(self)

# ===== CLI（簡易） =====
async def _amain():
    import argparse
    p = argparse.ArgumentParser(description="TwitCasting Recorder Core (Chrome Singleton)")
    p.add_argument("url", nargs="?", help="TwitCasting URL")
    p.add_argument("--login-setup", action="store_true")
    p.add_argument("--test", action="store_true")
    p.add_argument("--duration", type=int, default=0)
    p.add_argument("--headed", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    cfg = Config.load()
    if args.headed:
        cfg.headless = False
    if args.debug:
        cfg.debug_mode = True
        cfg.save_network_log = True
    if args.verbose:
        cfg.verbose_log = True
    cfg.save()

    rec = TwitCastingRecorder()
    try:
        # 【修正】URLも--login-setupも無い時は初期化せずに状態確認のみ
        if args.login_setup:
            await rec.initialize()
            ok = await rec.setup_login()
            print(json.dumps({"login_setup": ok}, ensure_ascii=False))
        elif args.url:
            await rec.initialize()
            du = (args.duration or None) if not args.test else (cfg.test_duration or 10)
            res = await rec.record(args.url, duration=du)
            print(json.dumps(res, ensure_ascii=False, indent=2))
        else:
            # 【修正】初期化なしで状態確認（副作用ゼロ）
            status = await rec.test_login_status()
            print(json.dumps({"login_status": status}, ensure_ascii=False))
    finally:
        await rec.close()

if __name__ == "__main__":
    import asyncio as _asyncio
    _asyncio.run(_amain())