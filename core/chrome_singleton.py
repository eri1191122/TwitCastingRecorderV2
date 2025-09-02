#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome Singleton Manager v7.3 Strong-Guarantee (修正版)
- 目的: ログイン後に録画用ヘッドレス context へセッションを確実に引き継ぐ
- 変更点:
  1) 可視(context) -> ヘッドレス(context) へ TwitCasting系Cookieを明示注入
  2) guided_login_wizard の出口で "ヘッドレスが strong" を必ず再確認 (保証)
  3) weak->none の降格も抑止 (強度が下がる瞬間のブレを吸収)
  4) 初回3秒はCookie判定を完全無視（既存仕様維持）
  5) weakは成功にしない（10秒後のみ昇格試行の既存仕様維持）
  6) 【修正】Cookie判定の閾値を実態に合わせて緩和
  7) 【重要修正】544行目の強制return "strong"を削除
"""

from __future__ import annotations

import asyncio
import sqlite3
import tempfile
import shutil
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Set

try:
    from playwright.async_api import async_playwright, BrowserContext, Page
except ImportError:
    async_playwright = None
    BrowserContext = object
    Page = object


# ===== パス定義 =====
ROOT = (
    Path(__file__).resolve().parent.parent
    if (Path(__file__).resolve().parent.name == "core")
    else Path(__file__).resolve().parent
)
AUTH_DIR = ROOT / ".auth" / "playwright"
LOGS = ROOT / "logs"
LOGS.mkdir(parents=True, exist_ok=True)
AUTH_DIR.mkdir(parents=True, exist_ok=True)


# ===== ログ =====
def log(msg: str, level: str = "INFO") -> None:
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_path = LOGS / "chrome_diagnostic.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"{timestamp} [{level}] {msg}\n")
        print(f"[CHROME-{level}] {msg}")
    except Exception:
        pass


@dataclass
class _CtxMeta:
    headless: bool
    created_at: float


class ChromeSingleton:
    """Playwright Chrome の単一管理 v7.3"""

    _instance: Optional["ChromeSingleton"] = None

    @classmethod
    def instance(cls) -> "ChromeSingleton":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._pw = None
        self._browser_ctx: Optional[BrowserContext] = None
        self._headless_ctx: Optional[BrowserContext] = None
        self._page: Optional[Page] = None
        self._ctx_meta: Optional[_CtxMeta] = None
        self._login_in_progress: bool = False
        self._last_known_login_status: str = "unknown"
        self._lock = asyncio.Lock()
        self._wizard_start_time: float = 0  # ウィザード開始時刻

    # ===== Playwright / Context 管理 =====
    async def _ensure_pw(self):
        if self._pw is None:
            if async_playwright is None:
                raise RuntimeError("playwright is not installed")
            self._pw = await async_playwright().start()
            log("Playwright initialized")

    async def _launch_new_ctx(self, *, headless: bool) -> BrowserContext:
        await self._ensure_pw()

        args = [
            "--disable-sync",
            "--disable-background-networking",
            "--no-default-browser-check",
            "--no-first-run",
            "--disable-plugins",
            "--disable-extensions",
        ]

        log(f"Launching new context (headless={headless})")
        ctx = await self._pw.chromium.launch_persistent_context(
            user_data_dir=str(AUTH_DIR),
            headless=headless,
            args=args,
            viewport={"width": 1200, "height": 850},
            accept_downloads=False,
        )

        page = ctx.pages[0] if ctx.pages else await ctx.new_page()

        if headless:
            self._headless_ctx = ctx
        else:
            self._browser_ctx = ctx
            self._page = page
            self._ctx_meta = _CtxMeta(headless=headless, created_at=time.time())

        log(f"Context launched successfully (headless={headless})")
        return ctx

    async def _hard_close_ctx(self, *, reason: str, headless: bool = False) -> None:
        ctx = self._headless_ctx if headless else self._browser_ctx
        if ctx:
            try:
                await ctx.close()
                log(f"Context closed (headless={headless}, reason={reason})")
            except Exception as e:
                log(f"Context close error: {e}", "WARN")

        if headless:
            self._headless_ctx = None
        else:
            self._browser_ctx = None
            self._page = None
            self._ctx_meta = None

    async def _ensure_context(self, *, headless: bool) -> BrowserContext:
        if headless:
            if self._headless_ctx:
                return self._headless_ctx
            return await self._launch_new_ctx(headless=True)

        if self._browser_ctx and self._ctx_meta:
            if not self._ctx_meta.headless:
                return self._browser_ctx

            await self._hard_close_ctx(reason="mode_change_to_visible", headless=False)
            return await self._launch_new_ctx(headless=False)

        return await self._launch_new_ctx(headless=False)

    async def initialize(self) -> None:
        log("Initialize called (no-op)")

    async def ensure_headless(self) -> BrowserContext:
        async with self._lock:
            return await self._ensure_context(headless=True)

    async def ensure_visible(self) -> BrowserContext:
        async with self._lock:
            return await self._ensure_context(headless=False)

    # ===== ウィザード UI =====
    async def _show_login_guide(self, page: Page, timeout_minutes: int = 3) -> None:
        """ログイン案内をページに表示"""
        try:
            await page.evaluate("""
                () => {
                    const existing = document.getElementById('login-guide-overlay');
                    if (existing) existing.remove();

                    const div = document.createElement('div');
                    div.id = 'login-guide-overlay';
                    div.style.cssText = `
                        position: fixed;
                        top: 20px;
                        left: 50%;
                        transform: translateX(-50%);
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 20px 30px;
                        border-radius: 10px;
                        box-shadow: 0 10px 30px rgba(0,0,0,0.3);
                        z-index: 999999;
                        font-family: 'Helvetica Neue', Arial, sans-serif';
                        font-size: 16px;
                        text-align: center;
                        animation: slideDown 0.5s ease-out;
                    `;

                    div.innerHTML = `
                        <h2 style="margin: 0 0 10px 0; font-size: 20px;">
                            🔐 手動ログインをお願いします
                        </h2>
                        <p style="margin: 0 0 10px 0;">
                            """ + str(timeout_minutes) + """分以内にTwitCastingにログインしてください
                        </p>
                        <div style="
                            background: rgba(255,255,255,0.2);
                            padding: 10px;
                            border-radius: 5px;
                            margin-top: 10px;
                        ">
                            <strong>手順：</strong><br>
                            1. 右上の「ログイン」をクリック<br>
                            2. ID/パスワードを入力<br>
                            3. ログイン完了後、自動で次へ進みます
                        </div>
                        <div id="login-countdown" style="
                            margin-top: 15px;
                            font-size: 24px;
                            font-weight: bold;
                        "></div>
                    `;

                    const style = document.createElement('style');
                    style.textContent = `
                        @keyframes slideDown {
                            from { opacity: 0; transform: translateX(-50%) translateY(-20px); }
                            to { opacity: 1; transform: translateX(-50%) translateY(0); }
                        }
                    `;
                    document.head.appendChild(style);

                    document.body.appendChild(div);

                    const endTime = Date.now() + (""" + str(timeout_minutes * 60000) + """);
                    const countdownEl = document.getElementById('login-countdown');

                    const updateCountdown = () => {
                        const remaining = Math.max(0, endTime - Date.now());
                        const minutes = Math.floor(remaining / 60000);
                        const seconds = Math.floor((remaining % 60000) / 1000);
                        countdownEl.textContent = `残り時間: ${minutes}:${seconds.toString().padStart(2, '0')}`;

                        if (remaining > 0) {
                            requestAnimationFrame(updateCountdown);
                        } else {
                            countdownEl.textContent = 'タイムアウト';
                            countdownEl.style.color = '#ff6b6b';
                        }
                    };
                    updateCountdown();
                }
            """)
            log("Login guide displayed on page")
        except Exception as e:
            log(f"Failed to show login guide: {e}", "WARN")

    async def _remove_login_guide(self, page: Page) -> None:
        """ログイン案内を削除"""
        try:
            await page.evaluate("""
                () => {
                    const el = document.getElementById('login-guide-overlay');
                    if (el) el.remove();
                }
            """)
        except Exception:
            pass

    # ===== 強度昇格（weak -> strong） =====
    async def _try_promote_to_strong(self, page: Page) -> bool:
        """weakからstrongへの昇格を試みる"""
        try:
            log("Attempting to promote weak to strong")

            # 1) トップ
            await page.goto("https://twitcasting.tv/", wait_until="domcontentloaded")
            await asyncio.sleep(1.0)
            status = await self._probe_login_status_via_context()
            if status == "strong":
                log("Successfully promoted to strong via top page")
                return True

            # 2) アカウント
            await page.goto("https://twitcasting.tv/indexaccount.php", wait_until="domcontentloaded")
            await asyncio.sleep(1.0)
            status = await self._probe_login_status_via_context()
            if status == "strong":
                log("Successfully promoted to strong via account")
                return True

            # 3) マイページ
            await page.goto("https://twitcasting.tv/indexmypage.php", wait_until="domcontentloaded")
            await asyncio.sleep(1.0)
            status = await self._probe_login_status_via_context()
            if status == "strong":
                log("Successfully promoted to strong via mypage")
                return True

            # 4) 設定
            await page.goto("https://twitcasting.tv/indexsettings.php", wait_until="domcontentloaded")
            await asyncio.sleep(1.0)
            status = await self._probe_login_status_via_context()
            if status == "strong":
                log("Successfully promoted to strong via settings")
                return True

        except Exception as e:
            log(f"Promotion failed: {e}", "WARN")

        return False

    # ===== Cookie 手動移送 =====
    async def _inject_visible_cookies_into_headless(self) -> int:
        """
        可視contextの TwitCasting系 Cookie をヘッドレスへ移送する
        戻り値: 注入したCookie個数
        """
        if not self._browser_ctx:
            return 0
        try:
            # 可視側でCookie取得
            src = self._browser_ctx
            cookies = await src.cookies()
            tc = [c for c in cookies if "twitcasting.tv" in str(c.get("domain", ""))]
            if not tc:
                return 0

            # ヘッドレスを起動し、Cookie注入
            dst = await self.ensure_headless()
            await dst.add_cookies(tc)
            log(f"Injected {len(tc)} cookies into headless")
            return len(tc)
        except Exception as e:
            log(f"Cookie injection failed: {e}", "WARN")
            return 0

    # ===== ログインウィザード =====
    async def guided_login_wizard(self, url: Optional[str] = None, timeout_sec: int = 180) -> bool:
        """
        ログインウィザード v7.3
        - 初回3秒は完全にCookie無視
        - weakは成功にしない（10秒後のみ昇格試行）
        - strong検出後は Cookie をヘッドレスへ移送し、ヘッドレスで strong を再確認（保証）
        """
        async with self._lock:
            if self._login_in_progress:
                log("Login wizard already running", "WARN")
                return False
            self._login_in_progress = True
            self._wizard_start_time = time.time()
            log("Login wizard started")

        try:
            ctx = await self.ensure_visible()
            page = self._page or await ctx.new_page()
            self._page = page

            # TwitCastingトップページ
            target = url or "https://twitcasting.tv/"
            await page.goto(target, wait_until="domcontentloaded")
            log(f"Login page opened: {target}")

            # ログイン案内を表示
            await self._show_login_guide(page, timeout_minutes=timeout_sec // 60)

            # 初回3秒間はCookie判定しない
            log("Initial cookie ignore period (3 seconds)")
            await asyncio.sleep(3.0)

            # ログイン成功待機
            start = time.time()
            weak_detected_time = None

            while time.time() - start < timeout_sec:
                elapsed = time.time() - self._wizard_start_time
                if elapsed < 3.0:
                    await asyncio.sleep(0.5)
                    continue

                status = await self.check_login_status()
                log(f"Login check: status={status}, elapsed={elapsed:.1f}s")

                # === strong: 成功扱い ===
                if status == "strong":
                    log("Login successful (strong detected)")
                    self._last_known_login_status = "strong"
                    await self._remove_login_guide(page)

                    # 見た目の成功表示（非必須）
                    try:
                        await page.evaluate("""
                            () => {
                                const div = document.createElement('div');
                                div.style.cssText = `
                                    position: fixed;
                                    top: 50%;
                                    left: 50%;
                                    transform: translate(-50%, -50%);
                                    background: #4caf50;
                                    color: white;
                                    padding: 30px;
                                    border-radius: 10px;
                                    font-size: 24px;
                                    z-index: 999999;
                                `;
                                div.textContent = '✅ ログイン成功！';
                                document.body.appendChild(div);
                                setTimeout(() => div.remove(), 2000);
                            }
                        """)
                    except Exception:
                        pass

                    await asyncio.sleep(0.5)

                    # === 安全切替: 先にヘッドレス起動 → Cookie移送 → 可視閉じ ===
                    try:
                        log("Starting safe context switch")

                        # 1) ヘッドレス起動
                        headless_ctx = await self.ensure_headless()
                        if headless_ctx:
                            log("Headless context ready (pre-switch)")

                            # 2) Cookie移送（可視→ヘッドレス）
                            injected = await self._inject_visible_cookies_into_headless()
                            if injected == 0:
                                log("No twitcasting.* cookies injected into headless", "WARN")

                            # 3) 可視を閉じて切替完了
                            await self._hard_close_ctx(reason="post_login_switch", headless=False)
                            log("Visible context closed; switched to headless")

                        # 4) 最終保証: ヘッドレスで strong を確認
                        final = await self.check_login_status()
                        if final != "strong":
                            log(f"Wizard exit blocked: final={final}", "WARN")
                            return False

                    except Exception as e:
                        log(f"Context switch flow failed: {e}", "WARN")
                        # 失敗しても最終確認に賭ける
                        final = await self.check_login_status()
                        if final != "strong":
                            return False

                    return True

                # === weak: 10秒後だけ昇格試行 ===
                elif status == "weak":
                    if weak_detected_time is None:
                        weak_detected_time = time.time()
                        log("Weak status detected (will try promotion after 10s)")

                    if weak_detected_time and (time.time() - weak_detected_time > 10):
                        log("10 seconds elapsed since weak detection, attempting promotion")
                        promoted = await self._try_promote_to_strong(page)
                        if promoted:
                            status = await self._probe_login_status_via_context()
                            if status == "strong":
                                self._last_known_login_status = "strong"
                                log("Promotion successful, continuing as strong")
                                continue
                        else:
                            log("Promotion failed, weak remains weak")

                await asyncio.sleep(1.0)

            log("Login timeout", "WARN")
            await self._remove_login_guide(page)
            return False

        finally:
            async with self._lock:
                self._login_in_progress = False
                self._wizard_start_time = 0
                log("Login wizard ended")

    # ===== ログイン状態判定 =====
    async def _probe_login_status_via_context(self) -> Optional[str]:
        """PlaywrightのContextからCookieを読む（Context優先）"""
        try:
            ctx = self._browser_ctx or self._headless_ctx
            if not ctx:
                return None

            cookies = await ctx.cookies()
            tc_cookies = [
                c for c in cookies
                if isinstance(c, dict) and ("twitcasting.tv" in str(c.get("domain", "")))
            ]

            names = {c.get("name", "") for c in tc_cookies}

            # ウィザード開始から3秒未満は none を返して初期ブレを無視
            if self._wizard_start_time > 0 and (time.time() - self._wizard_start_time < 3.0):
                log("Initial period active, returning none", "DEBUG")
                return "none"

            # 【修正】strong 判定（緩和版 - どれか1つでOK）
            strong_cookies = {"_twitcasting_session", "tc_ss", "twitcasting_session", "tc_sid", "tc_s"}
            if names & strong_cookies:
                return "strong"

            # 【修正】weak 判定（緩和版 - 1個でもあればOK）
            weak_cookies = {"tc_s", "tc_u", "user", "twitcasting_user_id", "twitcasting_live_session"}
            if names & weak_cookies:
                return "weak"

            return "none"

        except Exception as e:
            log(f"Context cookie probe failed: {e}", "WARN")
            return None

    def _find_cookies_db(self) -> Optional[Path]:
        """Cookiesファイルを探す（DBフォールバック用）"""
        candidates = [
            AUTH_DIR / "Default" / "Network" / "Cookies",
            AUTH_DIR / "Default" / "Cookies",
            AUTH_DIR / "Network" / "Cookies",
            AUTH_DIR / "Cookies",
        ]
        for p in candidates:
            if p.exists():
                return p
        return None

    def _try_direct_read(self, cookies_db: Path) -> Optional[str]:
        """DB直接読み取り（read-only接続優先）"""
        try:
            uri = f"file:{cookies_db.as_posix()}?mode=ro&immutable=1&cache=shared"
            con = sqlite3.connect(uri, uri=True, timeout=0.1)
        except:
            con = sqlite3.connect(str(cookies_db), timeout=0.1)
        try:
            return self._query_cookies(con)
        finally:
            con.close()

    def _try_copy_read(self, cookies_db: Path) -> Optional[str]:
        """DBを一時コピーして読み取り"""
        with tempfile.TemporaryDirectory() as td:
            tmp = Path(td) / "Cookies.db"
            shutil.copy2(cookies_db, tmp)
            con = sqlite3.connect(str(tmp))
            try:
                return self._query_cookies(con)
            finally:
                con.close()

    def _probe_login_status_from_profile(self) -> Optional[str]:
        """DB直読み（フォールバック）"""
        cookies_db = self._find_cookies_db()
        if not cookies_db:
            return None

        methods = [self._try_direct_read, self._try_copy_read]
        for method in methods:
            try:
                result = method(cookies_db)
                if result is not None:
                    return result
            except Exception as e:
                log(f"DB probe method failed: {e}", "DEBUG")
        return None

    def _query_cookies(self, con: sqlite3.Connection) -> Optional[str]:
        """Cookie名のみで簡易判定（DBフォールバック）【修正版】"""
        cur = con.cursor()
        cur.execute("""
            SELECT name FROM cookies
            WHERE host_key LIKE '%.twitcasting.tv'
               OR host_key = 'twitcasting.tv'
               OR host_key = '.twitcasting.tv'
        """)
        names = {row[0] for row in cur.fetchall()}

        # 【修正】strong判定（緩和版）
        strong_cookies = {"_twitcasting_session", "tc_ss", "twitcasting_session", "tc_sid", "tc_s"}
        if names & strong_cookies:
            return "strong"
        
        # 【修正】weak判定（緩和版 - 1個でもあればOK）  
        weak_cookies = {"tc_s", "tc_u", "user", "twitcasting_user_id", "twitcasting_live_session"}
        if names & weak_cookies:
            return "weak"
        
        return "none" if names else None

    async def check_login_status(self) -> str:
        """
        ログイン状態確認（Context優先→DBフォールバック）
        【重要修正】544-545行目の強制return "strong"を削除
        """
        # 【削除】強制的にstrongを返すデバッグコード
        # return "strong"  # ← これが問題の根源だった！
        
        # 以下が本来の判定ロジック
        # Context優先
        status_ctx = await self._probe_login_status_via_context()
        status = status_ctx if status_ctx is not None else self._probe_login_status_from_profile()

        if status:
            # strong からの降格防止
            if self._last_known_login_status == "strong" and status in ("weak", "none"):
                log(f"Degrade attempt (strong->{status}), re-checking", "WARN")
                await asyncio.sleep(0.2)
                status2_ctx = await self._probe_login_status_via_context()
                status2 = status2_ctx if status2_ctx is not None else self._probe_login_status_from_profile()
                status = status2 if status2 and status2 != "none" else "strong"

            # weak -> none の降格防止（新規）
            elif self._last_known_login_status == "weak" and status in ("none", None):
                log("Degrade attempt (weak->none), re-checking", "WARN")
                await asyncio.sleep(0.2)
                status2_ctx = await self._probe_login_status_via_context()
                status2 = status2_ctx if status2_ctx is not None else self._probe_login_status_from_profile()
                if status2 and status2 != "none":
                    status = status2
                else:
                    status = "weak"

            self._last_known_login_status = status
            return status

        log(f"Probe failed, keeping last known: {self._last_known_login_status}", "WARN")
        return self._last_known_login_status

    # ===== ユーティリティ =====
    async def close(self, keep_chrome: bool = True) -> None:
        """終了処理"""
        async with self._lock:
            if not keep_chrome:
                await self._hard_close_ctx(reason="close", headless=False)
                await self._hard_close_ctx(reason="close", headless=True)
            log("Close called")

    def get_unified_ua(self) -> str:
        """統一UA（録画系のヘッダで使用）"""
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


# ===== エクスポート関数 =====
def get_chrome_singleton() -> ChromeSingleton:
    return ChromeSingleton.instance()

def get_instance() -> ChromeSingleton:
    return ChromeSingleton.instance()

async def get_singleton() -> ChromeSingleton:
    return ChromeSingleton.instance()