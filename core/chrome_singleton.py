#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome Singleton Manager for TwitCasting Recorder
Version: 8.8.0 (完全自己修復型・NoneType.send対策版)

Single Chrome instance management with automatic recovery
機能削減なし・全機能維持、壊れたコンテキストを自動検出・再生成
NoneType.send即時リカバリ対応
"""

from __future__ import annotations
from pathlib import Path
import builtins as _bi
if not hasattr(_bi, "Path"):
    _bi.Path = Path

import asyncio
import json
import logging
import os
import sys
import threading
import time
import uuid
import traceback
from typing import Optional, Dict, Any, List

# ===== Playwright import =====
try:
    from playwright.async_api import async_playwright, Browser, BrowserContext, Page
    PlaywrightError = Exception
    print(f"[CHROME-INFO] Playwright import ok (python={sys.executable})")
except Exception as e:
    print(f"[ERROR] Playwright import failed: {e!r}")
    print(f"[INFO]  Python executable: {sys.executable}")
    traceback.print_exc()
    sys.exit(1)

# ===== パス設定 =====
ROOT = Path(__file__).resolve().parent.parent
LOGS = ROOT / "logs"
AUTH_DIR = ROOT / ".auth" / "playwright"

for d in (LOGS, AUTH_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ===== 診断ログ =====
class ChromeDiagnostics:
    @staticmethod
    def log(msg: str, level: str = "INFO") -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        log_path = LOGS / "chrome_diagnostic.log"
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"{ts} [{level}] {msg}\n")
            print(f"[CHROME-{level}] {msg}")
        except Exception as e:
            print(f"[CHROME-LOG-ERROR] {e}")

# ===== Singleton実装 =====
class ChromeSingleton:
    _instance = None
    _lock = threading.Lock()
    _instance_id = None

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance_id = str(uuid.uuid4())[:8]
                ChromeDiagnostics.log(f"ChromeSingleton created (ID: {cls._instance_id})", "INFO")
            return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return
        self._initialized = True

        self._playwright = None
        self._browser: Optional[Browser] = None
        self._browser_ctx: Optional[BrowserContext] = None
        self._headless_ctx: Optional[BrowserContext] = None
        self._browser_headless: Optional[bool] = None

        self._lock = threading.RLock()
        self._async_lock = asyncio.Lock()
        self._current_mode = None
        self._last_activity = time.time()
        
        # 自己修復カウンタ
        self._recovery_count = 0
        self._last_recovery = 0
        self._nonetype_recovery_count = 0  # NoneType.send専用カウンタ

        self._ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

        ChromeDiagnostics.log(f"ChromeSingleton initialized (ID: {self._instance_id})", "INFO")

    # ===== ヘルパーメソッド =====
    def _log(self, level: str, msg: str) -> None:
        ChromeDiagnostics.log(f"[{self._instance_id}] {msg}", level)

    def get_unified_ua(self) -> str:
        return self._ua

    # ===== 健全性チェック（改良版） =====
    async def _is_context_alive(self, ctx: Optional[BrowserContext]) -> bool:
        """コンテキストの死活確認（タイムアウト付き）"""
        if not ctx:
            return False
        
        try:
            # storage_stateで軽量チェック（2秒タイムアウト）
            await asyncio.wait_for(
                ctx.storage_state(),
                timeout=2.0
            )
            
            # ブラウザ接続確認
            br = getattr(ctx, "browser", None)
            if br and hasattr(br, "is_connected"):
                if not br.is_connected():
                    self._log("WARN", "Browser disconnected")
                    return False
            
            return True
            
        except asyncio.TimeoutError:
            self._log("WARN", "Context health check timeout")
            return False
        except Exception as e:
            self._log("WARN", f"Context health check failed: {e}")
            return False

    async def _safe_dispose_context(self, attr_name: str) -> None:
        """指定属性のコンテキストを安全に破棄"""
        try:
            ctx = getattr(self, attr_name, None)
            if ctx:
                try:
                    # ページを先に閉じる
                    if hasattr(ctx, 'pages'):
                        for page in ctx.pages:
                            try:
                                await page.close()
                            except:
                                pass
                    
                    # コンテキストを閉じる
                    await ctx.close()
                except Exception as e:
                    self._log("WARN", f"Close error suppressed ({attr_name}): {e}")
        finally:
            setattr(self, attr_name, None)

    # ===== ブラウザ管理 =====
    async def _ensure_playwright(self) -> None:
        """Playwright初期化"""
        if not self._playwright:
            self._playwright = await async_playwright().start()
            self._log("INFO", "Playwright started")

    async def _launch_browser(self, headless: bool = False) -> Browser:
        """ブラウザ起動（headless変更時は再起動）"""
        if self._browser:
            try:
                if self._browser.is_connected() and self._browser_headless == headless:
                    return self._browser
            except:
                pass
            
            # ブラウザ再起動が必要
            self._log("INFO", f"Browser restart required (headless: {self._browser_headless} -> {headless})")
            await self._close_browser_internal()

        await self._ensure_playwright()

        self._browser = await self._playwright.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
                '--start-maximized'
            ]
        )
        self._browser_headless = headless
        self._log("INFO", f"Browser launched (headless={headless})")
        return self._browser

    async def _close_browser_internal(self) -> None:
        """内部用：ブラウザとコンテキストを安全にクローズ"""
        await self._safe_dispose_context("_browser_ctx")
        await self._safe_dispose_context("_headless_ctx")
        
        try:
            if self._browser:
                if hasattr(self._browser, 'is_connected') and self._browser.is_connected():
                    await self._browser.close()
        except Exception as e:
            self._log("WARN", f"Browser close error: {e}")
        finally:
            self._browser = None
            self._browser_headless = None

    async def _create_context(self, headless: bool = False, persistent: bool = True) -> BrowserContext:
        """コンテキスト作成（NoneType.send対策済み）"""
        browser = await self._launch_browser(headless=headless)
        
        # コンテキスト作成オプション
        context_opts = {
            "user_agent": self._ua,
            "viewport": {'width': 1920, 'height': 1080},
            "locale": 'ja-JP',
            "timezone_id": 'Asia/Tokyo'
        }
        
        if persistent:
            state_file = AUTH_DIR / "state.json"
            if state_file.exists():
                context_opts["storage_state"] = str(state_file)

        # NoneType.send対策：3回リトライ
        for attempt in range(3):
            try:
                # ★ここが重要：NoneType.sendエラーを明示的にキャッチ
                context = await browser.new_context(**context_opts)
                
                if persistent:
                    self._log("INFO", f"Persistent context created (headless={headless})")
                else:
                    self._log("INFO", f"Temporary context created (headless={headless})")
                
                return context
                
            except AttributeError as e:
                # NoneType.send特有のエラーを検出
                error_str = str(e)
                if "NoneType" in error_str and "send" in error_str:
                    self._nonetype_recovery_count += 1
                    self._log("ERROR", f"NoneType.send detected (attempt {attempt+1}/3, total recovery: {self._nonetype_recovery_count})")
                    
                    if attempt < 2:
                        # 即座にemergency_restart
                        await self._emergency_restart()
                        # 短いバックオフ
                        await asyncio.sleep(0.5 * (attempt + 1))
                        # ブラウザ再取得
                        browser = await self._launch_browser(headless=headless)
                        continue
                    else:
                        raise RuntimeError(f"Failed to create context after 3 NoneType.send recoveries")
                else:
                    # その他のAttributeError
                    raise
                    
            except Exception as e:
                self._log("ERROR", f"Context creation failed (attempt {attempt+1}): {e}")
                if attempt == 2:
                    raise

        raise RuntimeError("Failed to create context after 3 attempts")

    # ===== 完全再起動（改良版） =====
    async def _emergency_restart(self) -> None:
        """Playwright/Browser完全再起動（NoneType.send対応）"""
        self._log("WARN", "Emergency restart initiated")
        self._recovery_count += 1
        self._last_recovery = time.time()
        
        # 全リソース破棄
        await self._safe_dispose_context("_headless_ctx")
        await self._safe_dispose_context("_browser_ctx")
        
        if self._browser:
            try:
                await self._browser.close()
            except:
                pass
            self._browser = None
            self._browser_headless = None
        
        if self._playwright:
            try:
                await self._playwright.stop()
            except:
                pass
            self._playwright = None
        
        # プロセス完全終了待ち
        await asyncio.sleep(1.0)
        
        # 再起動
        self._playwright = await async_playwright().start()
        self._log("INFO", f"Playwright restarted (recovery #{self._recovery_count}, NoneType recoveries: {self._nonetype_recovery_count})")

    # ===== モード切替（自己修復型） =====
    async def ensure_visible(self, persistent: bool = True) -> BrowserContext:
        """可視モード確保（3回リトライ）"""
        async with self._async_lock:
            for attempt in range(3):
                # 既存コンテキストの健全性チェック
                if self._current_mode == "visible" and self._browser_ctx:
                    if await self._is_context_alive(self._browser_ctx):
                        self._log("DEBUG", "Visible context healthy")
                        return self._browser_ctx
                    
                    self._log("WARN", f"Visible context dead (attempt {attempt+1})")
                    await self._safe_dispose_context("_browser_ctx")

                # ヘッドレスからCookie移行
                if self._current_mode == "headless" and self._headless_ctx:
                    await self._save_cookies_from_context(self._headless_ctx)

                try:
                    # 新規作成（NoneType.send対策済み）
                    self._browser_ctx = await self._create_context(headless=False, persistent=persistent)
                    
                    # Cookie移行
                    if self._headless_ctx:
                        try:
                            cookies = await self._headless_ctx.cookies()
                            if cookies:
                                await self._browser_ctx.add_cookies(cookies)
                                self._log("INFO", f"Migrated {len(cookies)} cookies to visible")
                        except Exception as e:
                            self._log("WARN", f"Cookie migration failed: {e}")

                    self._current_mode = "visible"
                    self._last_activity = time.time()
                    self._log("INFO", "Switched to visible mode")
                    return self._browser_ctx
                    
                except Exception as e:
                    self._log("ERROR", f"Visible context creation failed (attempt {attempt+1}): {e}")
                    if attempt == 2:
                        await self._emergency_restart()
            
            raise RuntimeError("Failed to create visible context after 3 attempts")

    async def ensure_headless(self, persistent: bool = True) -> BrowserContext:
        """ヘッドレスモード確保（必ず健全なコンテキストを返す）"""
        async with self._async_lock:
            for attempt in range(3):
                # 既存コンテキストの健全性チェック
                if self._current_mode == "headless" and self._headless_ctx:
                    if await self._is_context_alive(self._headless_ctx):
                        self._log("DEBUG", "Headless context healthy")
                        return self._headless_ctx
                    
                    self._log("WARN", f"Headless context dead (attempt {attempt+1})")
                    await self._safe_dispose_context("_headless_ctx")

                # 可視からCookie移行
                if self._current_mode == "visible" and self._browser_ctx:
                    try:
                        cookies = await self._browser_ctx.cookies()
                    except Exception as e:
                        cookies = []
                        self._log("WARN", f"Get cookies from visible failed: {e}")
                    
                    if not self._headless_ctx:
                        try:
                            self._headless_ctx = await self._create_context(headless=True, persistent=persistent)
                        except Exception as e:
                            self._log("ERROR", f"Headless creation failed: {e}")
                            if attempt == 2:
                                await self._emergency_restart()
                                continue
                            else:
                                continue
                    
                    if cookies:
                        try:
                            await self._headless_ctx.add_cookies(cookies)
                            self._log("INFO", f"Migrated {len(cookies)} cookies to headless")
                        except Exception as e:
                            self._log("WARN", f"Cookie migration to headless failed: {e}")

                try:
                    # コンテキスト作成（NoneType.send対策済み）
                    if not self._headless_ctx:
                        self._headless_ctx = await self._create_context(headless=True, persistent=persistent)
                    
                    # 作成直後の検証
                    await asyncio.wait_for(
                        self._headless_ctx.storage_state(),
                        timeout=2.0
                    )

                    self._current_mode = "headless"
                    self._last_activity = time.time()
                    self._log("INFO", "Switched to headless mode")
                    return self._headless_ctx
                    
                except Exception as e:
                    self._log("ERROR", f"Headless context creation failed (attempt {attempt+1}): {e}")
                    if attempt == 2:
                        # 最終手段：完全再起動
                        await self._emergency_restart()
            
            raise RuntimeError("Failed to create headless context after 3 attempts")

    # ===== Cookie管理 =====
    async def _save_cookies_from_context(self, ctx: BrowserContext) -> None:
        """コンテキストからCookie保存"""
        try:
            cookies = await ctx.cookies()
            cookie_file = LOGS / f"cookies_saved_{int(time.time())}.json"
            with open(cookie_file, "w", encoding="utf-8") as f:
                json.dump(cookies, f, ensure_ascii=False, indent=2)
            self._log("INFO", f"Saved {len(cookies)} cookies to {cookie_file.name}")
        except Exception as e:
            self._log("ERROR", f"Cookie save error: {e}")

    async def _inject_cookies_into_context(self, ctx: BrowserContext) -> None:
        """コンテキストへCookie注入"""
        try:
            cookie_files = sorted(LOGS.glob("cookies_saved_*.json"), key=lambda p: p.stat().st_mtime)
            if not cookie_files:
                return

            latest = cookie_files[-1]
            with open(latest, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            await ctx.add_cookies(cookies)
            self._log("INFO", f"Injected {len(cookies)} cookies from {latest.name}")
        except Exception as e:
            self._log("ERROR", f"Cookie injection error: {e}")

    async def _inject_visible_cookies_into_headless(self) -> int:
        """可視→ヘッドレスへCookie移送"""
        if not self._browser_ctx or not self._headless_ctx:
            return 0

        try:
            cookies = await self._browser_ctx.cookies(urls=[
                "https://twitcasting.tv/",
                "https://twitcasting.tv/mypage.php",
                "https://ssl.twitcasting.tv/"
            ])

            tc_cookies = [c for c in cookies if "twitcasting" in c.get("domain", "").lower()]

            if not tc_cookies:
                self._log("WARN", "No twitcasting cookies to inject")
                return 0

            cookie_names = [c.get("name", "") for c in tc_cookies]
            has_session = "_twitcasting_session" in cookie_names
            has_tc_ss = "tc_ss" in cookie_names

            self._log("INFO", f"Injecting {len(tc_cookies)} cookies to headless")
            self._log("INFO", f"Cookies: {', '.join(cookie_names)}")

            if not has_session:
                if has_tc_ss:
                    self._log("INFO", "Session via 'tc_ss' accepted as strong")
                else:
                    self._log("WARN", "Neither _twitcasting_session nor tc_ss found")

            await self._headless_ctx.add_cookies(tc_cookies)

            await asyncio.sleep(0.5)

            headless_cookies = await self._headless_ctx.cookies(urls=[
                "https://twitcasting.tv/",
                "https://twitcasting.tv/mypage.php"
            ])

            headless_tc = [c for c in headless_cookies if "twitcasting" in c.get("domain", "").lower()]
            headless_names = [c.get("name", "") for c in headless_tc]

            if "_twitcasting_session" not in headless_names:
                if "tc_ss" in headless_names:
                    self._log("INFO", "tc_ss confirmed in headless (sufficient for strong login)")
                else:
                    self._log("WARN", "No valid login cookies in headless")

                created = False
                try:
                    page = None
                    if self._headless_ctx.pages:
                        page = self._headless_ctx.pages[0]
                    else:
                        page = await self._headless_ctx.new_page()
                        created = True
                    try:
                        await page.goto("https://twitcasting.tv/mypage.php",
                                        wait_until="domcontentloaded", timeout=10000)
                        await asyncio.sleep(2)
                    except Exception as e:
                        self._log("WARN", f"Headless navigation error: {e}")

                    cookies2 = await self._headless_ctx.cookies(urls=["https://twitcasting.tv/"])
                    if any(c.get("name") == "_twitcasting_session" for c in cookies2):
                        self._log("INFO", "✅ _twitcasting_session obtained via page navigation")
                    elif any(c.get("name") == "tc_ss" for c in cookies2):
                        self._log("INFO", "✅ tc_ss confirmed (sufficient for strong login)")
                finally:
                    try:
                        if created and page:
                            await page.close()
                    except Exception:
                        pass
            else:
                self._log("INFO", f"✅ Headless has _twitcasting_session")

            return len(tc_cookies)

        except Exception as e:
            self._log("ERROR", f"Cookie injection failed: {e}")
            return 0

    # ===== ログイン管理 =====
    async def check_login_status(self) -> str:
        """ログイン状態確認（副作用なし）"""
        try:
            state_file = AUTH_DIR / "state.json"
            if not state_file.exists():
                return "none"

            with open(state_file, "r", encoding="utf-8") as f:
                state = json.load(f)

            cookies = state.get("cookies", [])
            cookie_names = {c.get("name", "") for c in cookies}

            primary = {"tc_ss", "_twitcasting_session", "tc_s"}
            secondary = {"tc_id", "tc_u"}

            if cookie_names & primary:
                if "tc_ss" in cookie_names and "_twitcasting_session" not in cookie_names:
                    self._log("INFO", "Login status: strong (tc_ss present, _twitcasting_session missing)")
                return "strong"
            elif cookie_names & secondary:
                return "weak"
            else:
                return "none"

        except Exception as e:
            self._log("ERROR", f"Login status check error: {e}")
            return "none"

    async def guided_login_wizard(self, timeout: float = 180.0) -> bool:
        """ログインウィザード"""
        self._log("INFO", "Starting guided login wizard")

        try:
            ctx = await self.ensure_visible(persistent=True)
            page = await ctx.new_page()

            candidates = [
                "https://twitcasting.tv/indexcaslogin.php",
                "https://ssl.twitcasting.tv/login.php",
                "https://twitcasting.tv/?m=login",
                "https://twitcasting.tv/login.php",
                "https://twitcasting.tv/",
            ]
            opened = False
            for u in candidates:
                try:
                    await page.goto(u, wait_until="domcontentloaded", timeout=10000)
                    title = await page.title()
                    has_form = await page.evaluate(
                        "()=>!!document.querySelector('form input[type=\"password\"],[name=\"password\"]')"
                    )
                    if "Not Found" not in (title or "") or has_form:
                        opened = True
                        break
                except Exception:
                    pass
            if not opened:
                self._log("ERROR", "Login page navigation failed (all candidates)")
                await page.close()
                return False

            self._log("INFO", "Opened TwitCasting login page")
            print("\n" + "="*50)
            print("ブラウザでTwitCastingにログインしてください")
            print("ログイン完了後、自動的に処理が続行されます")
            print("="*50 + "\n")

            start_time = time.time()
            while time.time() - start_time < timeout:
                await asyncio.sleep(2)

                cookies = await ctx.cookies()
                cookie_names = {c.get("name", "") for c in cookies}

                primary = {"tc_ss", "_twitcasting_session", "tc_s"}
                if cookie_names & primary:
                    self._log("INFO", "Login successful (strong detected)")

                    try:
                        await page.goto("https://twitcasting.tv/mypage.php",
                                        wait_until="domcontentloaded",
                                        timeout=10000)
                        await asyncio.sleep(2)
                        self._log("INFO", "Mypage navigation completed")
                    except Exception as e:
                        self._log("WARN", f"Mypage navigation error (non-fatal): {e}")

                    try:
                        await page.goto("https://twitcasting.tv/",
                                        wait_until="domcontentloaded",
                                        timeout=10000)
                        await asyncio.sleep(1)
                    except Exception:
                        pass

                    session_found = False
                    for i in range(20):
                        try:
                            cookies = await ctx.cookies(urls=[
                                "https://twitcasting.tv/",
                                "https://twitcasting.tv/mypage.php"
                            ])
                            tc_cookies = [c for c in cookies if "twitcasting" in c.get("domain", "").lower()]
                            names = [c.get("name", "") for c in tc_cookies]

                            session_found = "_twitcasting_session" in names
                            if session_found:
                                self._log("INFO", f"✅ _twitcasting_session found after {i*0.5}s")
                                self._log("DEBUG", f"All cookies: {names}")
                                break

                            if i == 19:
                                if "tc_ss" in names:
                                    self._log("INFO", "Session via 'tc_ss' confirmed after 10s")
                                    self._log("INFO", f"Available cookies: {names}")
                                else:
                                    self._log("WARN", "No valid login cookies after 10s")
                                    self._log("WARN", f"Available cookies: {names}")

                            await asyncio.sleep(0.5)
                        except Exception:
                            break

                    await ctx.storage_state(path=str(AUTH_DIR / "state.json"))
                    self._log("INFO", "Login state saved")

                    await asyncio.sleep(1.0)
                    self._log("INFO", "Starting safe context switch")

                    await self.ensure_headless(persistent=True)
                    await self._inject_visible_cookies_into_headless()

                    await page.close()

                    return True

            self._log("WARN", f"Login timeout after {timeout} seconds")
            await page.close()
            return False

        except Exception as e:
            self._log("ERROR", f"Login wizard error: {e}")
            return False

    # ===== Cookie出力（RecorderWrapper用） =====
    async def export_cookies(self, output_path: Path) -> bool:
        """現在のCookieをNetscape形式で出力"""
        try:
            # 現在のコンテキストから取得
            ctx = self._headless_ctx or self._browser_ctx
            if not ctx:
                self._log("ERROR", "No active context for cookie export")
                return False
                
            cookies = await ctx.cookies()
            tc_cookies = [c for c in cookies if "twitcasting" in c.get("domain", "").lower()]
            
            # Netscape形式で出力
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("# Netscape HTTP Cookie File\n")
                f.write("# This is a generated file! Do not edit.\n\n")
                
                for cookie in tc_cookies:
                    domain = cookie.get("domain", "")
                    flag = "TRUE" if domain.startswith(".") else "FALSE"
                    path = cookie.get("path", "/")
                    secure = "TRUE" if cookie.get("secure", False) else "FALSE"
                    expires = str(int(cookie.get("expires", 0)))
                    name = cookie.get("name", "")
                    value = cookie.get("value", "")
                    
                    f.write(f"{domain}\t{flag}\t{path}\t{secure}\t{expires}\t{name}\t{value}\n")
            
            self._log("INFO", f"Exported {len(tc_cookies)} cookies to {output_path}")
            return True
            
        except Exception as e:
            self._log("ERROR", f"Cookie export failed: {e}")
            return False

    # ===== 初期化（RecorderWrapper用） =====
    async def initialize(self) -> None:
        """初期化（ensure_headlessのエイリアス）"""
        await self.ensure_headless(persistent=True)

    # ===== ログイン実行（RecorderWrapper用） =====
    async def perform_login(self) -> bool:
        """ログイン実行（guided_login_wizardのエイリアス）"""
        return await self.guided_login_wizard()

    # ===== クリーンアップ =====
    async def close(self) -> None:
        """リソース解放"""
        self._log("INFO", "Closing ChromeSingleton")
        
        try:
            await self._safe_dispose_context("_browser_ctx")
            await self._safe_dispose_context("_headless_ctx")
            
            try:
                if self._browser:
                    if hasattr(self._browser, 'is_connected'):
                        if self._browser.is_connected():
                            await self._browser.close()
                    else:
                        await self._browser.close()
            except Exception as e:
                self._log("WARN", f"Browser close error: {e}")
            finally:
                self._browser = None
                self._browser_headless = None
            
            try:
                if self._playwright:
                    await self._playwright.stop()
            except Exception as e:
                self._log("WARN", f"Playwright stop error: {e}")
            finally:
                self._playwright = None
            
            self._current_mode = None
            self._log("INFO", "ChromeSingleton closed successfully")
            
        except Exception as e:
            self._log("WARN", f"Close finalization warning: {e}")

    # ===== 健全性情報 =====
    def is_healthy(self) -> bool:
        """システム健全性チェック"""
        try:
            # 最近のリカバリが多すぎないか
            if self._recovery_count > 5:
                if time.time() - self._last_recovery < 300:  # 5分以内に5回以上
                    return False
            
            # NoneType.sendリカバリが多すぎないか
            if self._nonetype_recovery_count > 10:
                return False
            
            # 長時間アイドルでないか
            if time.time() - self._last_activity > 3600:  # 1時間
                return False
            
            return True
            
        except:
            return False

# ===== グローバル取得関数 =====
def get_chrome_singleton() -> ChromeSingleton:
    """ChromeSingletonインスタンス取得"""
    return ChromeSingleton()

# ===== 互換性のため（facade/tc_recorder_core用） =====
ChromeSingleton = ChromeSingleton

# ===== テスト =====
if __name__ == "__main__":
    async def test():
        chrome = get_chrome_singleton()

        status = await chrome.check_login_status()
        print(f"Login status: {status}")

        if status != "strong":
            success = await chrome.guided_login_wizard()
            print(f"Login wizard result: {success}")

        ctx = await chrome.ensure_headless()
        page = await ctx.new_page()
        await page.goto("https://twitcasting.tv/")
        print(f"Page title: {await page.title()}")
        await page.close()

        print(f"Chrome is healthy: {chrome.is_healthy()}")

        await chrome.close()

    asyncio.run(test())