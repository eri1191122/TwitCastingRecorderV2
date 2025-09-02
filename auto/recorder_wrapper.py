#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recorder Wrapper for TwitCasting Auto Recording
Version: 3.0.1 (Windows NotImplementedError修正版)

主要変更点:
- ログイン処理を統合（同一Chromeインスタンス内で完結）
- 【重要修正】WindowsSelectorEventLoopPolicyをコメントアウト
- エラーハンドリング強化
- コード構造の最適化
- 型ヒントの充実
- ドキュメント改善
"""
import asyncio
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, ClassVar
from enum import Enum
from dataclasses import dataclass, field

# 親ディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

# 【重要修正】WindowsSelectorEventLoopPolicyをコメントアウト
# NotImplementedErrorの原因となるため削除
# if sys.platform.startswith("win"):
#     import asyncio as _asyncio
#     try:
#         _asyncio.set_event_loop_policy(_asyncio.WindowsSelectorEventLoopPolicy())
#     except Exception:
#         pass

# ==================== 定数定義 ====================
DEFAULT_MAX_CONCURRENT = 1
DEFAULT_TIMEOUT = 30.0
LOG_ROTATION_DAYS = 7
LOGIN_CHECK_INTERVAL = 300  # 5分ごとにログイン状態確認

# ==================== ディレクトリ設定 ====================
BASE_DIR = Path(__file__).parent.parent
LOGS_DIR = BASE_DIR / "logs"
COOKIES_DIR = LOGS_DIR / "cookies"
RECORDINGS_DIR = BASE_DIR / "recordings"

# ディレクトリ作成
for directory in [LOGS_DIR, COOKIES_DIR, RECORDINGS_DIR]:
    directory.mkdir(parents=True, exist_ok=True)


# ==================== Enum定義 ====================
class TargetPrefix(Enum):
    """ターゲットプレフィックス定義"""
    CHANNEL = "c:"      # 通常チャンネル
    GOOGLE = "g:"       # Google連携アカウント
    INSTAGRAM = "ig:"   # Instagram連携（将来実装）
    FACEBOOK = "f:"     # Facebook連携（将来実装）


class RecordingStatus(Enum):
    """録画状態"""
    IDLE = "idle"
    PREPARING = "preparing"
    RECORDING = "recording"
    FINALIZING = "finalizing"
    ERROR = "error"


# ==================== データクラス ====================
@dataclass
class RecordingJob:
    """録画ジョブ情報"""
    job_id: str
    target: str
    url: str
    status: RecordingStatus = RecordingStatus.IDLE
    started_at: float = field(default_factory=time.time)
    duration: Optional[int] = None
    output_files: List[str] = field(default_factory=list)
    error: Optional[str] = None


# ==================== メインクラス ====================
class RecorderWrapper:
    """
    TwitCasting録画ラッパー（リファクタリング版）
    - ログイン処理統合
    - URL生成の修正実装
    - 同時実行数管理
    - エラーハンドリング強化
    """
    
    # クラス変数
    _max_concurrent: ClassVar[int] = DEFAULT_MAX_CONCURRENT
    _semaphore: ClassVar[Optional[asyncio.Semaphore]] = None
    _recording_jobs: ClassVar[Dict[str, RecordingJob]] = {}
    _count_lock: ClassVar[Optional[asyncio.Lock]] = None
    _last_login_check: ClassVar[float] = 0
    _recorder_instance: ClassVar[Optional[Any]] = None  # TwitCastingRecorderの単一インスタンス
    
    @classmethod
    def configure(cls, max_concurrent: int = DEFAULT_MAX_CONCURRENT) -> None:
        """
        設定変更
        
        Args:
            max_concurrent: 最大同時実行数（1-10）
        """
        if not 1 <= max_concurrent <= 10:
            raise ValueError(f"max_concurrent must be 1-10, got {max_concurrent}")
        cls._max_concurrent = max_concurrent
        cls._semaphore = None  # 再生成を強制
        cls._log_event("configuration_changed", {"max_concurrent": max_concurrent})
        
    @classmethod
    def _ensure_semaphore(cls) -> asyncio.Semaphore:
        """セマフォの遅延生成（ループセーフ）"""
        if cls._semaphore is None:
            cls._semaphore = asyncio.Semaphore(cls._max_concurrent)
        return cls._semaphore
        
    @classmethod
    def _ensure_lock(cls) -> asyncio.Lock:
        """ロックの遅延生成（ループセーフ）"""
        if cls._count_lock is None:
            cls._count_lock = asyncio.Lock()
        return cls._count_lock
        
    @classmethod
    async def _ensure_recorder(cls) -> Any:
        """
        レコーダーインスタンスの確保（シングルトン）
        同一Chromeインスタンスを使い回すための重要な変更
        """
        if cls._recorder_instance is None:
            from facade import TwitCastingRecorder
            cls._recorder_instance = TwitCastingRecorder()
            await cls._recorder_instance.initialize()
            cls._log_event("recorder_initialized", {"type": "singleton"})
        return cls._recorder_instance
        
    @classmethod
    async def _ensure_login(cls) -> bool:
        """
        ログイン状態の確保
        必要に応じて自動ログインを実行
        
        Returns:
            ログイン成功: True, 失敗: False
        """
        recorder = await cls._ensure_recorder()
        
        # ログイン状態確認
        status = await recorder.test_login_status()
        cls._log_event("login_check", {"status": status})
        
        if status == "strong":
            cls._last_login_check = time.time()
            return True
            
        # ログインが必要
        print("[INFO] ログインが必要です。ブラウザが開きます...")
        cls._log_event("login_required", {"current_status": status})
        
        try:
            success = await recorder.setup_login()
            if success:
                print("✅ ログイン成功！")
                cls._last_login_check = time.time()
                cls._log_event("login_success", {})
                return True
            else:
                print("❌ ログイン失敗")
                cls._log_event("login_failed", {})
                return False
        except Exception as e:
            cls._log_event("login_error", {"error": str(e)})
            print(f"[ERROR] ログインエラー: {e}")
            return False
            
    @classmethod
    async def start_record(
        cls,
        target: str,
        *,
        hint_url: Optional[str] = None,
        duration: Optional[int] = None,
        job_id: Optional[str] = None,
        force_login_check: bool = False
    ) -> Dict[str, Any]:
        """
        録画開始（ログイン統合版）
        
        Args:
            target: ターゲット識別子（例: "g:117191215409354941008"）
            hint_url: URL直接指定（オプション）
            duration: 録画時間（秒）
            job_id: ジョブID（未指定時は自動生成）
            force_login_check: 強制的にログイン確認を実行
            
        Returns:
            録画結果の辞書
        """
        start_time = time.time()
        
        # Job ID生成
        if not job_id:
            safe_target = target.replace(":", "_").replace("@", "_")
            timestamp = int(time.time() * 1000)
            job_id = f"{safe_target}_{timestamp}"
            
        # ジョブ情報作成
        job = RecordingJob(
            job_id=job_id,
            target=target,
            url=hint_url or "",
            duration=duration
        )
        
        # 開始ログ
        cls._log_event("start_request", {
            "job_id": job_id,
            "target": target,
            "duration": duration,
            "current_count": len(cls._recording_jobs),
            "max_concurrent": cls._max_concurrent
        })
        
        # セマフォ取得
        semaphore = cls._ensure_semaphore()
        
        try:
            await asyncio.wait_for(
                semaphore.acquire(),
                timeout=DEFAULT_TIMEOUT
            )
        except asyncio.TimeoutError:
            cls._log_event("semaphore_timeout", {
                "job_id": job_id,
                "timeout_sec": DEFAULT_TIMEOUT
            })
            return cls._create_error_result(job, "max_concurrent_timeout", start_time)
            
        # 録画ジョブ登録
        async with cls._ensure_lock():
            cls._recording_jobs[job_id] = job
            job.status = RecordingStatus.PREPARING
            
        print(f"[WRAPPER] Starting [{len(cls._recording_jobs)}/{cls._max_concurrent}]: {target} (job={job_id})")
        
        try:
            # URL構築
            url = cls._build_url(target, hint_url)
            if not url:
                cls._log_event("url_build_failed", {"job_id": job_id, "target": target})
                return cls._create_error_result(job, "invalid_target_format", start_time)
                
            job.url = url
            cls._log_event("url_built", {"job_id": job_id, "url": url, "target": target})
            
            # ログイン確認（5分ごと or 強制確認）
            if force_login_check or (time.time() - cls._last_login_check > LOGIN_CHECK_INTERVAL):
                login_ok = await cls._ensure_login()
                if not login_ok:
                    return cls._create_error_result(job, "login_failed", start_time)
            
            # レコーダー取得（既存インスタンス使い回し）
            recorder = await cls._ensure_recorder()
            
            # 録画実行
            job.status = RecordingStatus.RECORDING
            result = await recorder.record(url, duration=duration)
            
            # 結果処理
            success = result.get("success", False)
            elapsed = time.time() - start_time
            
            if success:
                job.status = RecordingStatus.FINALIZING
                job.output_files = result.get("output_files", [])
                
                cls._log_event("recording_complete", {
                    "job_id": job_id,
                    "success": True,
                    "elapsed_sec": elapsed,
                    "output_files": job.output_files
                })
                
                print(f"[WRAPPER] Success [{len(cls._recording_jobs)}/{cls._max_concurrent}]: {job_id} ({elapsed:.1f}s)")
                
                return {
                    "ok": True,
                    "job_id": job_id,
                    "reason": None,
                    "output_files": job.output_files,
                    "elapsed_sec": elapsed,
                    "m3u8": result.get("m3u8"),
                    "target": target,
                    "url": url
                }
            else:
                job.status = RecordingStatus.ERROR
                job.error = result.get("error", "unknown_error")
                
                cls._log_event("recording_failed", {
                    "job_id": job_id,
                    "error": job.error,
                    "elapsed_sec": elapsed
                })
                
                print(f"[WRAPPER] Failed [{len(cls._recording_jobs)}/{cls._max_concurrent}]: {job_id} - {job.error}")
                
                return cls._create_error_result(job, job.error, start_time, details=result)
                
        except asyncio.CancelledError:
            cls._log_event("job_cancelled", {"job_id": job_id})
            return cls._create_error_result(job, "cancelled", start_time)
            
        except Exception as e:
            cls._log_event("unexpected_error", {
                "job_id": job_id,
                "error": str(e),
                "type": e.__class__.__name__
            })
            return cls._create_error_result(
                job, 
                f"exception:{e.__class__.__name__}", 
                start_time,
                error=str(e)
            )
            
        finally:
            # ジョブ削除
            async with cls._ensure_lock():
                cls._recording_jobs.pop(job_id, None)
                    
            # セマフォ解放
            semaphore.release()
            
            print(f"[WRAPPER] Released [{len(cls._recording_jobs)}/{cls._max_concurrent}]")
            
    @staticmethod
    def _build_url(target: str, hint_url: Optional[str] = None) -> Optional[str]:
        """
        URL構築（修正版）
        
        Args:
            target: ターゲット識別子
            hint_url: 直接URL指定
            
        Returns:
            構築されたURL or None（エラー時）
        """
        # 直接URL指定優先
        if hint_url:
            return hint_url
            
        # バリデーション
        if not target or len(target) < 3:
            return None
            
        # プレフィックス分離
        prefix, suffix = target[:2], target[2:]
        if not suffix:
            return None
            
        # URL生成
        url_mapping = {
            TargetPrefix.CHANNEL.value: f"https://twitcasting.tv/{suffix}",
            TargetPrefix.GOOGLE.value: f"https://twitcasting.tv/g:{suffix}",
            TargetPrefix.INSTAGRAM.value: f"https://twitcasting.tv/ig:{suffix}",
            TargetPrefix.FACEBOOK.value: f"https://twitcasting.tv/f:{suffix}",
        }
        
        return url_mapping.get(prefix)
            
    @classmethod
    def _create_error_result(
        cls,
        job: RecordingJob,
        reason: str,
        start_time: float,
        **kwargs
    ) -> Dict[str, Any]:
        """
        エラー結果の生成（共通化）
        
        Args:
            job: 録画ジョブ
            reason: エラー理由
            start_time: 開始時刻
            **kwargs: 追加情報
            
        Returns:
            エラー結果の辞書
        """
        job.status = RecordingStatus.ERROR
        job.error = reason
        
        result = {
            "ok": False,
            "job_id": job.job_id,
            "reason": reason,
            "elapsed_sec": time.time() - start_time,
            "target": job.target,
            "url": job.url
        }
        result.update(kwargs)
        return result
            
    @classmethod
    def _log_event(cls, event: str, data: dict) -> None:
        """
        イベントログ出力（JSONL形式）
        
        Args:
            event: イベント名
            data: ログデータ
        """
        try:
            # 日付別ファイル
            log_file = LOGS_DIR / f"wrapper_{datetime.now():%Y%m%d}.jsonl"
            
            # エントリ作成
            entry = {
                "timestamp": datetime.now().isoformat(),
                "event": event,
                **data
            }
            
            # アトミック書き込み
            with open(log_file, "a", encoding="utf-8") as f:
                json.dump(entry, f, ensure_ascii=False, separators=(",", ":"))
                f.write("\n")
                
        except Exception as e:
            print(f"[WRAPPER] Log error: {e}", file=sys.stderr)
            
    @classmethod
    def get_status(cls) -> Dict[str, Any]:
        """
        現在のステータス取得
        
        Returns:
            ステータス情報
        """
        jobs_info = []
        for job_id, job in cls._recording_jobs.items():
            jobs_info.append({
                "job_id": job_id,
                "target": job.target,
                "status": job.status.value,
                "duration": job.duration,
                "elapsed": time.time() - job.started_at
            })
            
        return {
            "max_concurrent": cls._max_concurrent,
            "recording_count": len(cls._recording_jobs),
            "recording_jobs": jobs_info,
            "semaphore_available": cls._max_concurrent - len(cls._recording_jobs),
            "last_login_check": datetime.fromtimestamp(cls._last_login_check).isoformat() if cls._last_login_check else None,
            "recorder_active": cls._recorder_instance is not None
        }
        
    @classmethod
    async def cleanup_old_logs(cls, days: int = LOG_ROTATION_DAYS) -> int:
        """
        古いログファイル削除
        
        Args:
            days: 保持日数
            
        Returns:
            削除されたファイル数
        """
        deleted = 0
        cutoff = time.time() - (days * 86400)
        
        try:
            for log_file in LOGS_DIR.glob("wrapper_*.jsonl"):
                if log_file.stat().st_mtime < cutoff:
                    log_file.unlink()
                    deleted += 1
                    
            cls._log_event("log_cleanup", {
                "deleted_files": deleted,
                "retention_days": days
            })
            
        except Exception as e:
            cls._log_event("log_cleanup_error", {"error": str(e)})
            
        return deleted
        
    @classmethod
    async def shutdown(cls) -> None:
        """
        シャットダウン処理
        レコーダーインスタンスのクリーンアップ
        """
        if cls._recorder_instance:
            try:
                await cls._recorder_instance.close(keep_chrome=False)
                cls._recorder_instance = None
                cls._log_event("shutdown_complete", {})
                print("[INFO] Recorder shutdown complete")
            except Exception as e:
                cls._log_event("shutdown_error", {"error": str(e)})
                print(f"[ERROR] Shutdown error: {e}")


# ==================== テストコード ====================
async def main():
    """メインテスト関数"""
    print("=== RecorderWrapper Test (v3.0.1) ===")
    print(f"Max concurrent: {RecorderWrapper._max_concurrent}")
    
    # URL生成テスト
    print("\n--- URL Build Test ---")
    test_cases = [
        ("c:teruto_nico", "通常チャンネル"),
        ("g:117191215409354941008", "Google連携アカウント"),
        ("ig:testuser", "Instagram連携（将来）"),
        ("invalid", "無効な形式"),
        ("c:", "suffix なし"),
    ]
    
    for target, description in test_cases:
        url = RecorderWrapper._build_url(target)
        print(f"{target:30} -> {url or 'None':50} # {description}")
        
    # 録画テスト
    print("\n--- Recording Test (with auto-login) ---")
    
    # 初回はログイン確認を強制
    result = await RecorderWrapper.start_record(
        "g:117191215409354941008",
        duration=10,  # 10秒テスト
        force_login_check=True  # 初回は強制ログイン確認
    )
    
    print(f"\nResult: ok={result.get('ok')}, reason={result.get('reason')}")
    if result.get('ok'):
        print(f"Output files: {result.get('output_files')}")
    
    # ステータス表示
    print("\n--- Status ---")
    print(json.dumps(RecorderWrapper.get_status(), indent=2, ensure_ascii=False))
    
    # シャットダウン
    await RecorderWrapper.shutdown()


if __name__ == "__main__":
    # テスト実行
    asyncio.run(main())