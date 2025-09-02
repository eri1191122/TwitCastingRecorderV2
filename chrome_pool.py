# chrome_pool.py - 重大バグ修正版
"""
Chrome Pool Manager - 商用レベル完全版
重大バグ修正：
1. ヘルスチェック多重起動防止
2. 生存判定の堅牢化
3. 再作成時の旧タスク確実停止
4. releaseでのタスク完全待機
"""
import asyncio
import time
import json
import os
from typing import Optional, Dict, Any
from pathlib import Path
from dataclasses import dataclass, field
from playwright.async_api import async_playwright, BrowserContext, Page
from datetime import datetime
from enum import Enum

class ContextState(Enum):
    """コンテキストの状態管理"""
    IDLE = "idle"
    RECORDING = "recording"
    LOGIN = "login"
    ERROR = "error"

@dataclass
class ContextMetrics:
    """メトリクス収集"""
    created_at: float = field(default_factory=time.time)
    last_used: float = field(default_factory=time.time)
    reuse_count: int = 0
    error_count: int = 0
    state_history: list = field(default_factory=list)

class ChromeContextPool:
    """
    Chrome単一管理＋プール思想
    重大バグ修正済み版
    """
    
    _instance = None
    _lock = asyncio.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._playwright = None
        self._context: Optional[BrowserContext] = None
        self._state = ContextState.IDLE
        self._metrics = ContextMetrics()
        self._pages: Dict[str, Page] = {}
        self._health_check_task = None
        self._initialized = True
    
    async def acquire(self, purpose: str = "recording") -> BrowserContext:
        """
        コンテキスト取得（用途別最適化）
        """
        async with self._lock:
            # 健全性チェック
            if not await self._is_healthy():
                await self._recreate()
            
            # 初回作成
            if self._context is None:
                await self._create()
            
            # 状態遷移
            self._transition_state(purpose)
            
            # メトリクス更新
            self._metrics.last_used = time.time()
            self._metrics.reuse_count += 1
            
            # デバッグ情報
            await self._log_status()
            
            return self._context
    
    async def _create(self):
        """初回作成（1回だけ）"""
        print(f"[ChromePool] Creating new context...")
        
        if self._playwright is None:
            self._playwright = await async_playwright().start()
        
        # ヘッドレス判定
        headless = self._should_be_headless()
        
        self._context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=str(Path(".auth/playwright").absolute()),
            headless=headless,
            viewport={"width": 1280, "height": 720},
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-features=TranslateUI",
            ],
            ignore_default_args=["--enable-automation"],
            chromium_sandbox=False,
        )
        
        self._metrics.created_at = time.time()
        print(f"[ChromePool] Context created successfully")
        
        # ヘルスチェック開始
        self._start_health_check()
    
    async def _is_healthy(self) -> bool:
        """
        健全性チェック（堅牢版）
        - context.is_closed()を最初にチェック
        - ページ0枚でもOK
        - 必ずページをclose
        """
        if self._context is None or self._context.is_closed():
            return False
        
        page = None
        try:
            # 新規ページで軽い評価（ページ0枚でも判定可能）
            page = await self._context.new_page()
            await page.evaluate("1 + 1")
            return True
        except Exception:
            self._metrics.error_count += 1
            return False
        finally:
            # 必ずページを閉じる（リーク防止）
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
    
    async def _recreate(self):
        """
        異常時の自己修復（完全版）
        - 旧ヘルスチェック確実停止
        - 順序を厳密化
        """
        print(f"[ChromePool] Self-healing initiated...")
        
        # 先にヘルスチェックを止める（多重ループ防止）
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
        
        # 既存コンテキストを安全に破棄
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
            self._context = None
        
        # 再作成（ここで_start_health_checkが安全に再起動される）
        await self._create()
    
    def _should_be_headless(self) -> bool:
        """賢いヘッドレス判定"""
        # 環境変数チェック
        if os.environ.get("FORCE_HEADLESS") == "true":
            return True
        if os.environ.get("FORCE_HEADED") == "true":
            return False
        
        # 設定ファイルチェック
        config_path = Path("config.json")
        if config_path.exists():
            try:
                config = json.loads(config_path.read_text())
                # ログイン必要なら一時的にheaded
                if config.get("needs_login", False):
                    return False
                return config.get("headless", True)
            except:
                pass
        
        # デフォルト
        return True
    
    def _transition_state(self, purpose: str):
        """状態遷移管理"""
        old_state = self._state
        
        if purpose == "recording":
            self._state = ContextState.RECORDING
        elif purpose == "login":
            self._state = ContextState.LOGIN
        else:
            self._state = ContextState.IDLE
        
        self._metrics.state_history.append({
            "from": old_state.value,
            "to": self._state.value,
            "timestamp": datetime.now().isoformat()
        })
    
    async def get_page(self, purpose: str = "default") -> Page:
        """用途別ページ取得（タブ管理）"""
        async with self._lock:
            # 既存ページがあれば再利用
            if purpose in self._pages:
                page = self._pages[purpose]
                if not page.is_closed():
                    return page
            
            # なければ新規作成
            ctx = await self.acquire(purpose)
            page = await ctx.new_page()
            self._pages[purpose] = page
            
            # ページ数制限（メモリ対策）
            await self._cleanup_pages()
            
            return page
    
    async def _cleanup_pages(self):
        """古いページを閉じる（メモリ管理）"""
        MAX_PAGES = 5
        
        if len(self._pages) > MAX_PAGES:
            # 最も古いページを閉じる
            oldest_purpose = list(self._pages.keys())[0]
            page = self._pages.pop(oldest_purpose)
            try:
                await page.close()
            except:
                pass
    
    def _start_health_check(self):
        """
        定期健康診断（多重起動防止版）
        - 既存タスクが動いていたら新規起動しない
        """
        # 既に動いていたら新しく起動しない
        if self._health_check_task and not self._health_check_task.done():
            print("[ChromePool] Health check already running, skipping...")
            return
        
        async def health_check_loop():
            """ヘルスチェックループ"""
            try:
                while True:
                    await asyncio.sleep(30)  # 30秒ごと
                    # ヘルス悪化時はロック下で自己修復
                    if not await self._is_healthy():
                        print("[ChromePool] Health check failed, recreating...")
                        async with self._lock:
                            await self._recreate()
            except asyncio.CancelledError:
                # 正常終了
                print("[ChromePool] Health check cancelled")
                pass
            except Exception as e:
                print(f"[ChromePool] Health check error: {e}")
        
        self._health_check_task = asyncio.create_task(health_check_loop())
        print("[ChromePool] Health check started")
    
    async def _log_status(self):
        """デバッグ用ステータス出力"""
        status = {
            "state": self._state.value,
            "reuse_count": self._metrics.reuse_count,
            "error_count": self._metrics.error_count,
            "uptime_seconds": int(time.time() - self._metrics.created_at),
            "pages_open": len(self._pages),
            "health_check_running": bool(self._health_check_task and not self._health_check_task.done()),
            "last_state_changes": self._metrics.state_history[-3:] if self._metrics.state_history else []
        }
        
        # ログファイル出力
        log_path = Path("logs/chrome_pool_status.json")
        log_path.parent.mkdir(exist_ok=True)
        log_path.write_text(json.dumps(status, indent=2))
    
    async def restart(self, headless: Optional[bool] = None):
        """明示的な再起動（ヘッドレス切り替え用）"""
        async with self._lock:
            # 環境変数で一時的に設定
            if headless is not None:
                os.environ["FORCE_HEADLESS"] = "true" if headless else "false"
            
            await self._recreate()
            
            # 環境変数クリア
            if "FORCE_HEADLESS" in os.environ:
                del os.environ["FORCE_HEADLESS"]
    
    def set_headless(self, value: bool):
        """次回起動のヘッドレス設定"""
        # config.jsonを更新
        config_path = Path("config.json")
        try:
            if config_path.exists():
                config = json.loads(config_path.read_text())
            else:
                config = {}
            config["headless"] = value
            config_path.write_text(json.dumps(config, indent=2))
        except Exception as e:
            print(f"[ChromePool] Failed to update config: {e}")
    
    def is_alive(self) -> bool:
        """生存確認"""
        return bool(self._context and not self._context.is_closed())
    
    async def record_guard(self):
        """録画の並列実行を抑止"""
        return _AsyncLockCtx(self._lock)
    
    def heartbeat(self, context_reused: bool):
        """メトリクス記録"""
        rec = {
            "ts": datetime.utcnow().isoformat() + "Z",
            "context_reused": context_reused,
            "reuse_count": self._metrics.reuse_count,
            "error_count": self._metrics.error_count,
            "state": self._state.value
        }
        try:
            log_path = Path("logs/heartbeat.json")
            log_path.parent.mkdir(exist_ok=True)
            log_path.write_text(json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
    
    async def release(self):
        """
        リソース解放（完全版）
        - ヘルスチェックタスクを確実に待機
        - すべてのリソースを順序良く解放
        """
        print("[ChromePool] Releasing resources...")
        
        # ヘルスチェックタスクを確実に停止
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
            print("[ChromePool] Health check stopped")
        
        # すべてのページを閉じる
        for purpose, page in list(self._pages.items()):
            try:
                await page.close()
            except Exception:
                pass
        self._pages.clear()
        
        # コンテキストを閉じる
        if self._context:
            try:
                await self._context.close()
            except Exception:
                pass
            self._context = None
        
        # Playwrightを停止
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None
        
        print("[ChromePool] Resources released")

class _AsyncLockCtx:
    """非同期コンテキストマネージャー"""
    def __init__(self, lock: asyncio.Lock):
        self._lock = lock
    
    async def __aenter__(self):
        await self._lock.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()

# グローバルインスタンス
chrome_pool = ChromeContextPool()