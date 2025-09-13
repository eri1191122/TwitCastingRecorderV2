#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TwitCasting Recorder Core v4.8.2 〈完全版：プロセス孤児化ゼロ・例外伝播ゼロ〉
- v4.8.1 の全機能を維持（機能削減なし）
- 【改善】_graceful_terminate を shield + suppress で完全静音化
- 【改善】_execute_ytdlp にキャンセル時の確実な後始末を追加
- 【維持】reader_task の await を shield で保護（キャンセル伝播防止）
- 【維持】BrowserContext の健全性検査とワンショット再生成（'NoneType.send' 撲滅）
- 【維持】M3U8検出強化（Content-Type対応・URLパターン拡張）
- 【維持】yt-dlp 実行パスの自動フォールバック／フォーマット再試行
- 【維持】ゲート処理・Cookie保存・デバッグログ
"""
from __future__ import annotations

# ==== パス初期化（core直下/直下配置の両対応） ====
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ==== 既存import（機能維持） ====
import asyncio
import contextlib
import json
import shutil
import time
import re
import uuid
import tempfile
import os
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

# ===== HLS判定（Content-Type対応＋パターン拡張） =====
def _is_real_hls(url: str, content_type: str = "") -> bool:
    """TwitCastingの本編HLSだけを通す判定（プレビュー等は除外）"""
    u = url.lower()

    # Content-Typeによる判定（最優先）
    if content_type:
        ct = content_type.lower()
        if any(x in ct for x in ["mpegurl", "vnd.apple.mpegurl", "x-mpegurl"]):
            deny_patterns = ("preview", "thumbnail", "thumb", "ad/", "/ads/", "test", "dummy")
            if not any(p in u for p in deny_patterns):
                return True

    # m3u8以外は即除外
    if ".m3u8" not in u:
        return False

    # 除外パターン（プレビュー・サムネ・広告・疑似プレイリスト）
    deny_patterns = (
        "preview", "thumbnail", "thumb", "ad/", "/ads/", "ad=",
        "preroll", "dash", "manifest", "test", "dummy", "sample",
        "error", "offline", "maintenance", "placeholder"
    )
    if any(pattern in u for pattern in deny_patterns):
        return False

    # 許可パターン（2025年対応）
    allow_patterns = (
        "/hls/", "/live/", "livehls", "tc.livehls", "/tc.hls/",
        "/streams/", "/media.m3u8", "/master.m3u8",
        "/hls-live/", "/hls_cdn/", "/hls-cdn/", "/hls_stream/",
        "/playlist.m3u8", "/index.m3u8", "/chunklist", "/variant"
    )

    # ドメイン厳格化
    valid_domains = (
        ".twitcasting.tv/" in u or ".twitcasting.net/" in u or
        "://twitcasting.tv/" in u or "://twitcasting.net/" in u
    )

    return valid_domains and any(p in u for p in allow_patterns)

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
                allowed = set(asdict(cls()).keys())  # 既定キーのみ受理
                filtered = {k: v for k, v in data.items() if k in allowed}
                base = asdict(cls())
                base.update(filtered)
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
    """strong/weak/none を返す（互換維持・未使用でも保持）"""
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
    """改善されたプロセス終了処理（完全静音化）"""
    if proc is None:
        return
    try:
        proc.terminate()
        # 【改善】shield + suppress で完全静音化
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await asyncio.shield(asyncio.wait_for(proc.wait(), timeout=soft_timeout))
        CoreDiagnostics.log("Process terminated gracefully", "DEBUG")
        return
    except ProcessLookupError:  # TimeoutErrorは上でsuppressされる
        pass
    except Exception as e:
        CoreDiagnostics.log(f"Terminate error: {e}", "WARN")
    
    # Kill if still running
    try:
        proc.kill()
        with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
            await asyncio.shield(asyncio.wait_for(proc.wait(), timeout=5.0))
        CoreDiagnostics.log("Process killed", "WARN")
    except ProcessLookupError:
        pass
    except Exception as e:
        CoreDiagnostics.log(f"Kill error: {e}", "ERROR")

def _compose_output_base(url: str) -> Path:
    user_id = _extract_user_id(url) or "unknown"
    user_id = re.sub(r'[:<>"|?*/\\]', '_', user_id)  # Windowsファイル名サニタイズ
    return RECORDINGS / f"{_now()}_{user_id}_{uuid.uuid4().hex[:8]}"

def _check_error_in_tail(tail: List[str]) -> bool:
    """403エラーや 0 bytes をチェック"""
    error_patterns = ["403", "Forbidden", "0 bytes", "ERROR"]
    return any(pattern in line for line in tail[-50:] for pattern in error_patterns)

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

    # --- HOTFIX: BrowserContext健全性検査（軽量→実動作の二段） ---
    async def _validate_ctx(self, ctx: BrowserContext) -> bool:
        try:
            # 軽量：storage_state が通るか
            await ctx.storage_state()
            # 実動作：new_page() → すぐclose
            tmp = await ctx.new_page()
            try:
                await tmp.close()
            except Exception:
                pass
            return True
        except Exception as e:
            CoreDiagnostics.log(f"_validate_ctx failed: {e}", "WARN")
            return False

    async def _reopen_headless_ctx(self) -> Optional[BrowserContext]:
        try:
            await self.chrome.close()  # 既存ブラウザごと安全クローズ
        except Exception:
            pass
        ctx = await self.chrome.ensure_headless()
        return ctx

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

    async def _pass_membership_gate(self, page: Page) -> bool:
        """入場/同意ボタンを自動クリック（configがtrueの時だけ）"""
        if not getattr(self.cfg, "enable_group_gate_auto", False):
            return False
        clicked = False
        candidates = [
            # テキスト（日本語/英語）
            "text=入場", "text=視聴する", "text=同意して進む", "text=続ける",
            "text=OK", "text=はい", "text=入室", "text=参加", "text=入る",
            "text=Enter", "text=Join", "text=Continue", "text=Watch", "text=Agree",
            # data / aria / ボタン / リンク / クラス
            "[data-test*='enter']", "[data-test*='join']", "[data-test*='gate']", "[data-test*='membership']",
            "[aria-label*='入場']", "[aria-label*='視聴']", "[aria-label*='参加']",
            "[aria-label*='Enter']", "[aria-label*='Join']",
            "button:has-text('入場')", "button:has-text('視聴')", "button:has-text('入室')", "button:has-text('参加')",
            "button:has-text('Enter')", "button:has-text('Join')", "button:has-text('Watch')",
            "a[href*='membershipjoinplans']", "a[href*='membership']", "a[href*='join']",
            "[class*='enter-button']", "[class*='gate-button']", "[class*='join-button']", "[class*='membership-button']", "[class*='tc-button']",
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

    # ===== M3U8キャプチャ（Content-Type対応） =====
    async def _capture_m3u8(self, page: Page, base_timeout: int = 20) -> Optional[str]:
        found = {"u": None}

        def on_resp(resp):
            try:
                u = resp.url
                try:
                    ct = resp.headers.get("content-type", "").lower()
                except Exception:
                    ct = ""

                if getattr(self.cfg, "save_network_log", False):
                    self._netlog.append({
                        "url": u,
                        "status": resp.status,
                        "content_type": ct,
                        "time": time.time(),
                        "is_real": _is_real_hls(u, ct)
                    })

                if not found["u"] and _is_real_hls(u, ct):
                    found["u"] = u
                    CoreDiagnostics.log(f"Real HLS detected: {u[:100]}... (CT: {ct})", "INFO")
            except Exception:
                pass

        page.on("response", on_resp)

        try:
            CoreDiagnostics.log("Waiting for player ready...", "DEBUG")
            try:
                await self._wait_for_player_ready(page)
            except Exception:
                pass

            gate_clicked = await self._pass_membership_gate(page)
            if gate_clicked:
                await page.wait_for_timeout(1500)
                try:
                    await self._wait_for_player_ready(page)
                except Exception:
                    pass

            CoreDiagnostics.log("Triggering playback...", "DEBUG")
            try:
                await self._trigger_playback(page)
            except Exception:
                pass

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
                        try:
                            await self._wait_for_player_ready(page)
                        except Exception:
                            pass
                        gate_clicked = await self._pass_membership_gate(page)
                        if gate_clicked:
                            await page.wait_for_timeout(1500)
                        await self._trigger_playback(page)

                    await page.wait_for_event(
                        "response",
                        predicate=lambda r: _is_real_hls(r.url, r.headers.get("content-type", "")),
                        timeout=short * 1000
                    )
                except Exception:
                    CoreDiagnostics.log(f"Strategy '{strategy}' timeout/failed", "DEBUG")
                    continue

            if found["u"]:
                CoreDiagnostics.log(f"Captured real HLS: {found['u']}", "SUCCESS")
            else:
                CoreDiagnostics.log("No real HLS found after all strategies", "WARNING")
                if getattr(self.cfg, "debug_mode", False) and self._netlog:
                    print("[CORE-DEBUG] Last captured URLs:")
                    for log in self._netlog[-20:]:
                        if ".m3u8" in log.get("url", ""):
                            is_real = log.get("is_real", False)
                            status = "✓" if is_real else "✗"
                            ct = log.get("content_type", "")
                            print(f"  {status} {log['url'][:100]} [CT: {ct}]")
        finally:
            try:
                page.remove_listener("response", on_resp)
                CoreDiagnostics.log("Response listener removed", "DEBUG")
            except Exception:
                pass

        return found["u"]

    # ===== yt-dlp 実行（完全版：キャンセル時の孤児化防止） =====
    async def _execute_ytdlp(
        self,
        m3u8: str,
        page: Page,
        out_tpl: str,
        cookies_path: Path,
        duration: Optional[int] = None,
        retry_count: int = 0
    ) -> Dict[str, Any]:
        # UA（chrome_singleton 統一 UA → 取得不能なら page 側）
        ua = getattr(self.chrome, "get_unified_ua", lambda: None)() or "Mozilla/5.0"
        try:
            if ua == "Mozilla/5.0":
                ua = await page.evaluate("navigator.userAgent")
        except Exception:
            pass

        # yt-dlpパス解決
        ytdlp_candidate = self.cfg.ytdlp_path or "yt-dlp"
        if shutil.which(ytdlp_candidate) or os.path.exists(ytdlp_candidate):
            ytdlp_launcher = [ytdlp_candidate]
        else:
            ytdlp_launcher = [sys.executable, "-m", "yt_dlp"]
            CoreDiagnostics.log(f"yt-dlp not in PATH, using: {' '.join(ytdlp_launcher)}", "INFO")

        cmd = [
            *ytdlp_launcher,
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

        val = (self.cfg.preferred_quality or "").strip()
        if val:
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

        # 【改善】キャンセル時の確実な後始末
        try:
            if duration and duration > 0:
                try:
                    await asyncio.wait_for(proc.wait(), timeout=duration + 60)
                except asyncio.TimeoutError:
                    await _graceful_terminate(proc)
            else:
                await proc.wait()
        except asyncio.CancelledError:
            # キャンセル時は必ずプロセスを終了
            await _graceful_terminate(proc)
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(reader_task)
            raise
        finally:
            # reader_task の確実な回収
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.shield(reader_task)

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

            # --- HOTFIX: Context取得→健全性確認→必要なら一回だけ再生成 ---
            ctx = await self.chrome.ensure_headless()
            if ctx is None:
                CoreDiagnostics.log("ensure_headless() returned None", "ERROR")
                return {"success": False, "error": "chrome_context_none",
                        "details": "chrome_singleton の ensure_headless() が失敗"}

            if not await self._validate_ctx(ctx):
                CoreDiagnostics.log("Headless context broken; reopening (phase1)", "WARN")
                ctx = await self._reopen_headless_ctx()
                if ctx is None or not await self._validate_ctx(ctx):
                    return {"success": False, "error": "ctx_unhealthy", "details": "validate_ctx twice failed"}

            page: Optional[Page] = None
            out_base = _compose_output_base(url)
            out_tpl = str(out_base) + ".%(ext)s"

            try:
                # --- HOTFIX: new_page 失敗も一回だけ護る ---
                try:
                    page = await ctx.new_page()
                except Exception as e:
                    CoreDiagnostics.log(f"ctx.new_page failed ({type(e).__name__}) — one-shot reopen", "WARN")
                    ctx = await self._reopen_headless_ctx()
                    if ctx is None:
                        return {"success": False, "error": "ctx_reopen_failed"}
                    page = await ctx.new_page()  # ここで失敗したら例外を上に返す

                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await self._wait_for_player_ready(page)

                # JIT #1: 入場時Cookie保存
                await _save_cookies_netscape(ctx, LOGS / f"cookies_enter_{_now()}.txt", self.cfg.cookie_domain)

                # m3u8 検出
                m3u8 = await self._capture_m3u8(page, base_timeout=self.cfg.m3u8_timeout)
                if not m3u8:
                    if self.cfg.save_network_log and self._netlog:
                        (LOGS / f"net_debug_{_now()}.json").write_text(
                            json.dumps(self._netlog, ensure_ascii=False, indent=2),
                            encoding="utf-8"
                        )
                    return {"success": False, "error": "M3U8_NOT_FOUND"}

                # JIT #2: m3u8直前Cookie
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

                    cookies_path_retry = LOGS / f"cookies_retry_{_now()}.txt"
                    await _save_cookies_netscape(ctx, cookies_path_retry, self.cfg.cookie_domain)

                    if bad_format:
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
                        exec_result = await self._execute_ytdlp(
                            m3u8, page, out_tpl, cookies_path_retry, duration, retry_count=1
                        )

                    rc = exec_result["return_code"]
                    tail = exec_result["tail"]

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
        副作用ゼロ版、initialize()呼ばない
        """
        try:
            status = await self.chrome.check_login_status()
            print(f"[DEBUG] Login status: {status}")
            CoreDiagnostics.log(f"Login status check: {status}", "INFO")
            return status if status in ("strong", "weak", "none") else "none"
        except Exception as e:
            print(f"[ERROR] Login check error: {e}")
            CoreDiagnostics.log(f"Login check error: {e}", "ERROR")
            return "none"

    async def record(self, url: str, duration: Optional[int] = None, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
            status = await rec.test_login_status()
            print(json.dumps({"login_status": status}, ensure_ascii=False))
    finally:
        await rec.close()

if __name__ == "__main__":
    import asyncio as _asyncio
    _asyncio.run(_amain())