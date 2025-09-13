#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Facade for TwitCastingRecorder v2.1 (エンジン初期化保証版)
- 初期化を完全遅延（GUI起動時にChrome起動しない）
- ChromeSingletonとの完璧な連携
- meta引数の互換性確保
- ログイン後のエンジン初期化を確実に実行
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
    GUI互換性のためのメインクラス（v2.1）
    - 完全な遅延初期化
    - ChromeSingletonの新仕様に完全対応
    - meta引数の互換性確保
    - エンジン初期化の確実な実行
    """
    
    def __init__(self) -> None:
        """コンストラクタ（Chrome起動しない）"""
        self._initialized = False
        self.is_recording = False
        self._record_lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()  # 初期化の排他制御
        
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
        冪等性を保証（何度呼んでも安全）
        """
        async with self._init_lock:  # 排他制御
            # 既に初期化済みかつエンジンも存在すれば何もしない
            if self._initialized and self._engine is not None:
                logger.debug("Already initialized with engine")
                return
                
            try:
                # 【重要】常にヘッドレスで初期化（見えないように）
                await self.chrome.ensure_headless()
                
                # Core初期化（エンジンがない場合のみ作成）
                if self._engine is None:
                    from tc_recorder_core import RecordingEngine
                    self._engine = RecordingEngine(self.chrome)
                    logger.info("RecordingEngine created successfully")
                
                self._initialized = True
                logger.info("Facade initialized successfully (headless)")
            except Exception as e:
                logger.error(f"Failed to initialize: {e}")
                # 失敗時は状態をリセット
                self._initialized = False
                self._engine = None
                raise InitializationError(f"Initialization failed: {e}") from e
            
    async def setup_login(self) -> bool:
        """
        ログインセットアップ
        ChromeSingletonが自動で可視→ヘッドレス切り替え
        成功時は必ずエンジンも初期化する
        """
        try:
            # ログインウィザード実行（Chrome側で可視→ヘッドレス自動切り替え）
            result = await self.chrome.guided_login_wizard()
            logger.info(f"Login setup result: {result}")
            
            # 成功時は完全初期化（エンジン生成を保証）
            if result:
                logger.info("Login successful, initializing engine...")
                await self.initialize()  # エンジンを確実に作る
                
                # エンジンが正常に作られたか確認
                if self._engine is None:
                    logger.error("Engine creation failed after login")
                    return False
                    
                logger.info("Engine initialized after login")
                
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
            
    async def record(self, url: str, duration: Optional[int] = None, **kwargs) -> Dict[str, Any]:
        """
        録画実行
        初回のみ初期化（遅延初期化）
        エンジンの存在を確実に保証
        
        Args:
            url: 録画対象URL
            duration: 録画時間（秒）
            **kwargs: 追加パラメータ（meta等、将来の拡張用）
                     - meta: メタデータ辞書（現在は未使用だが受け取る）
        """
        async with self._record_lock:
            if self.is_recording:
                raise AlreadyRecordingError("Recording already in progress")
                
            self.is_recording = True
            try:
                # 【重要】初期化条件を厳格化：フラグまたは実体どちらか欠けても初期化
                if (not self._initialized) or (self._engine is None):
                    logger.info("Initializing before recording (initialized=%s, engine=%s)", 
                               self._initialized, self._engine is not None)
                    await self.initialize()
                
                # エンジンの存在を再確認（安全網）
                if self._engine is None:
                    logger.error("Engine still None after initialization attempt")
                    return {
                        "success": False,
                        "error": "Engine initialization failed",
                        "tail": []
                    }
                
                # kwargsから既知のパラメータを抽出（将来の拡張用）
                meta = kwargs.pop('meta', None)
                
                # metaが渡された場合はデバッグログ
                if meta:
                    logger.debug(f"Received meta data (currently unused): {meta}")
                
                # 未知のkwargsがあれば警告（デバッグ用）
                if kwargs:
                    logger.debug(f"Unknown kwargs passed to record(): {list(kwargs.keys())}")
                
                # 録画エンジン経由で実行
                logger.info(f"Starting recording: {url} (duration={duration})")
                result = await self._engine.record(url, duration)
                
                # 結果のログ
                success = result.get('success', False)
                if success:
                    logger.info(f"Recording completed successfully: {url}")
                else:
                    logger.warning(f"Recording failed: {url} - {result.get('error', 'Unknown error')}")
                    
                return result
                
            except Exception as e:
                logger.error(f"Recording error: {e}", exc_info=True)
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
            self._engine = None
            self._core = None
            
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
        
    # ======================= デバッグ用メソッド =======================
    def get_status(self) -> Dict[str, Any]:
        """
        現在の状態を取得（デバッグ用）
        """
        return {
            "initialized": self._initialized,
            "engine_exists": self._engine is not None,
            "is_recording": self.is_recording,
            "chrome_exists": self.chrome is not None,
        }

# ======================= テスト =======================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def test():
        print("=== Facade Test v2.1 ===")
        rec = TwitCastingRecorder()
        
        # 初期状態確認
        print("0. Initial status:")
        print(f"   {rec.get_status()}")
        
        # 状態確認（Chrome起動しない）
        print("\n1. Checking login status...")
        status = await rec.test_login_status()
        print(f"   Login status: {status}")
        print(f"   Current status: {rec.get_status()}")
        
        # ログインセットアップ（必要時のみ）
        if status == "none":
            print("\n2. Starting login setup...")
            result = await rec.setup_login()
            print(f"   Login result: {result}")
            print(f"   After login status: {rec.get_status()}")
        
        # テスト録画（meta引数も渡してみる）
        print("\n3. Test recording with meta...")
        test_meta = {"test": "data", "timestamp": "2025-09-07"}
        result = await rec.test_record("https://twitcasting.tv/test")
        print(f"   Recording result: {result.get('success')}")
        print(f"   After recording status: {rec.get_status()}")
        
        # meta付き録画テスト
        print("\n4. Recording with meta parameter...")
        result2 = await rec.record("https://twitcasting.tv/test", duration=5, meta=test_meta)
        print(f"   Recording with meta result: {result2.get('success')}")
        
        # クリーンアップ
        await rec.close(keep_chrome=False)
        print("\n=== Test Complete ===")
        
    asyncio.run(test())