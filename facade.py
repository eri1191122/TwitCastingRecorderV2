#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Facade for TwitCastingRecorder v2.0 (最終完成版)
- 初期化を完全遅延（GUI起動時にChrome起動しない）
- ChromeSingletonとの完璧な連携
- 不要コード削除でシンプル化
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Dict, Any, Union
from pathlib import Path

logger = logging.getLogger(__name__)

# ======================= 例外クラス =======================
class FacadeError(Exception):
    """Facade基底例外"""
    pass

class AlreadyRecordingError(FacadeError):
    """既に録画中エラー"""
    pass

class InitializationError(FacadeError):
    """初期化エラー"""
    pass

# ======================= 補助関数 =======================
def get_config_class():
    """Configクラスを取得"""
    try:
        from tc_recorder_core import Config
        return Config
    except Exception as e:
        logger.warning(f"Could not import Config: {e}")
        
        class DummyConfig:
            def __init__(self):
                self.headless = True  # デフォルトはヘッドレス
                self.ffmpeg_path = ""
                self.ytdlp_path = "yt-dlp"
                self.keep_alive_interval = 1800
                self.preferred_quality = "res,codec:avc:iseq"
                self.default_duration = 600
                self.test_duration = 10
                self.debug_mode = False
                self.verbose_log = False
                self.save_network_log = False
                self.enable_group_gate_auto = False
                self.auto_heal = True
                self.m3u8_timeout = 45
                self.cookie_domain = ".twitcasting.tv"
                
            @classmethod
            def load(cls):
                return cls()
                
            def save(self):
                pass
                
        return DummyConfig

def get_paths() -> Dict[str, Path]:
    """パス情報を取得"""
    try:
        from tc_recorder_core import ROOT, RECORDINGS, LOGS
        return {
            "ROOT": ROOT,
            "RECORDINGS": RECORDINGS,
            "LOGS": LOGS
        }
    except Exception as e:
        logger.warning(f"Could not import paths: {e}")
        base = Path(__file__).resolve().parent
        return {
            "ROOT": base,
            "RECORDINGS": base / "recordings",
            "LOGS": base / "logs",
        }

# ======================= メインクラス =======================
class TwitCastingRecorder:
    """
    GUI互換性のためのメインクラス（最終版）
    - 完全な遅延初期化
    - ChromeSingletonの新仕様に完全対応
    """
    
    def __init__(self) -> None:
        """コンストラクタ（Chrome起動しない）"""
        self._initialized = False
        self.is_recording = False
        self._record_lock = asyncio.Lock()
        
        # ChromeSingleton取得（起動はしない）
        try:
            from core.chrome_singleton import get_chrome_singleton
        except ImportError:
            from chrome_singleton import get_chrome_singleton
            
        self.chrome = get_chrome_singleton()  # インスタンス取得のみ
        
        # Config取得
        Config = get_config_class()
        self.cfg = Config.load()
        
        # Core録画エンジン（遅延初期化）
        self._core = None
        self._engine = None
        
        logger.debug("Facade TwitCastingRecorder created (no Chrome launch)")
        
    async def initialize(self) -> None:
        """
        初期化（必要時のみ呼ばれる）
        常にヘッドレスで初期化
        """
        if self._initialized:
            return
            
        try:
            # 【重要】常にヘッドレスで初期化（見えないように）
            await self.chrome.ensure_headless()
            
            # Core初期化
            from tc_recorder_core import RecordingEngine
            self._engine = RecordingEngine(self.chrome)
            
            self._initialized = True
            logger.info("Facade initialized successfully (headless)")
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            raise InitializationError(f"Initialization failed: {e}") from e
            
    async def setup_login(self) -> bool:
        """
        ログインセットアップ
        ChromeSingletonが自動で可視→ヘッドレス切り替え
        """
        try:
            # ログインウィザード実行（Chrome側で可視→ヘッドレス自動切り替え）
            result = await self.chrome.guided_login_wizard()
            logger.info(f"Login setup result: {result}")
            
            # 成功時は初期化済みとマーク
            if result:
                self._initialized = True
                
            return bool(result)
        except Exception as e:
            logger.error(f"Login setup error: {e}")
            return False
            
    async def test_login_status(self) -> Union[bool, str]:
        """
        ログイン状態確認（副作用ゼロ）
        Chrome起動しない
        """
        try:
            # ChromeSingletonの状態確認（副作用なし）
            result = await self.chrome.check_login_status()
            
            # 結果を文字列形式に正規化
            if result in ["strong", "weak", "none", "unknown"]:
                return result
            elif result is True:
                return "strong"
            else:
                return "none"
                
        except Exception as e:
            logger.error(f"Login status check error: {e}")
            return "none"
            
    async def record(self, url: str, duration: Optional[int] = None) -> Dict[str, Any]:
        """
        録画実行
        初回のみ初期化（遅延初期化）
        """
        async with self._record_lock:
            if self.is_recording:
                raise AlreadyRecordingError("Recording already in progress")
                
            self.is_recording = True
            try:
                # 【重要】初回のみ初期化（ヘッドレス）
                if not self._initialized:
                    await self.initialize()
                
                # 録画エンジン経由で実行
                if self._engine:
                    result = await self._engine.record(url, duration)
                else:
                    # エンジンがない場合はエラー
                    result = {"success": False, "error": "Engine not initialized"}
                    
                logger.info(f"Recording completed: success={result.get('success', False)}")
                return result
                
            except Exception as e:
                logger.error(f"Recording error: {e}")
                return {
                    "success": False,
                    "error": str(e),
                    "tail": []
                }
            finally:
                self.is_recording = False
                
    async def test_record(self, url: str) -> Dict[str, Any]:
        """テスト録画（10秒）"""
        duration = self.cfg.test_duration or 10
        logger.info(f"Starting test recording ({duration} seconds)")
        return await self.record(url, duration=duration)
        
    async def close(self, keep_chrome: bool = True, **kwargs) -> None:
        """
        クローズ処理
        keep_chrome=Trueの場合、Chromeは閉じない（GUI用）
        """
        logger.info(f"Closing facade (keep_chrome={keep_chrome})")
        
        # Chrome完全終了の場合のみ
        if not keep_chrome:
            await self.chrome.close()
            self._initialized = False
            self.is_recording = False
            
        logger.info("Facade closed")
        
    # ======================= プロパティ（後方互換） =======================
    @property
    def session(self):
        """後方互換性用（廃止予定）"""
        class _DummySession:
            def __init__(self, recorder):
                self.cfg = recorder.cfg
                self.is_recording = recorder.is_recording
        return _DummySession(self)
        
    @property
    def engine(self):
        """録画エンジン参照"""
        return self._engine

# ======================= テスト =======================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        print("=== Facade Test ===")
        rec = TwitCastingRecorder()
        
        # 状態確認（Chrome起動しない）
        print("1. Checking login status...")
        status = await rec.test_login_status()
        print(f"   Login status: {status}")
        
        # ログインセットアップ（必要時のみ）
        if status == "none":
            print("2. Starting login setup...")
            result = await rec.setup_login()
            print(f"   Login result: {result}")
        
        # テスト録画
        print("3. Test recording...")
        result = await rec.test_record("https://twitcasting.tv/test")
        print(f"   Recording result: {result.get('success')}")
        
        # クリーンアップ
        await rec.close(keep_chrome=False)
        print("=== Test Complete ===")
        
    asyncio.run(test())