#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job Queue for TwitCasting Auto Recording
Version: 2.0.0 (Ultimate Edition)

高度な優先度付きジョブキュー（拡張可能設計）
- importパス完全統一
- エラーハンドリング強化
- リファクタリング実施
"""
import asyncio
import json
import time
import heapq
import logging
from pathlib import Path
from typing import Optional, Dict, Any, List, Set, Tuple
from enum import Enum, auto
from dataclasses import dataclass, field, asdict
from datetime import datetime
import sys

# プロジェクトルートをパスに追加
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# auto.パッケージからimport（統一）
try:
    from auto.recorder_wrapper import RecorderWrapper
except ImportError as e:
    print(f"[FATAL] Failed to import RecorderWrapper: {e}", file=sys.stderr)
    sys.exit(1)

# ==================== ロギング設定 ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ==================== 定数定義 ====================
class QueueConfig:
    """キュー設定定数"""
    DEFAULT_MAX_CONCURRENT: int = 2
    DEFAULT_MAX_RETRIES: int = 3
    DEFAULT_RETRY_DELAY: int = 10
    DEFAULT_WORKER_SLEEP: float = 1.0
    DEFAULT_QUEUE_SIZE_LIMIT: int = 1000
    DEFAULT_JOB_TIMEOUT: int = 3600  # 1時間


# ==================== Enum定義 ====================
class JobPriority(Enum):
    """ジョブ優先度（数値が小さいほど優先度高）"""
    CRITICAL = 1  # 最優先（緊急配信など）
    HIGH = 2      # 高優先度（通常配信）
    NORMAL = 3    # 通常
    LOW = 4       # 低優先度（リトライなど）
    BACKGROUND = 5  # バックグラウンド
    
    def __lt__(self, other):
        """優先度比較"""
        if not isinstance(other, JobPriority):
            return NotImplemented
        return self.value < other.value


class JobStatus(Enum):
    """ジョブステータス"""
    PENDING = auto()    # 待機中
    RUNNING = auto()    # 実行中
    COMPLETED = auto()  # 完了
    FAILED = auto()     # 失敗
    CANCELLED = auto()  # キャンセル
    RETRYING = auto()   # リトライ中
    EXPIRED = auto()    # 期限切れ


# ==================== データクラス ====================
@dataclass(order=True)
class RecordingJob:
    """優先度付き録画ジョブ（拡張版）"""
    # 比較用フィールド（優先度とタイムスタンプ）
    priority: int = field(compare=True)
    created_at: float = field(default_factory=time.time, compare=True)
    
    # 基本情報
    job_id: str = field(compare=False)
    target: str = field(compare=False)
    duration: Optional[int] = field(default=None, compare=False)
    
    # 時刻情報
    started_at: Optional[float] = field(default=None, compare=False)
    completed_at: Optional[float] = field(default=None, compare=False)
    expires_at: Optional[float] = field(default=None, compare=False)
    
    # ステータス情報
    status: JobStatus = field(default=JobStatus.PENDING, compare=False)
    result: Optional[Dict[str, Any]] = field(default=None, compare=False)
    error: Optional[str] = field(default=None, compare=False)
    
    # リトライ情報
    retry_count: int = field(default=0, compare=False)
    max_retries: int = field(default=QueueConfig.DEFAULT_MAX_RETRIES, compare=False)
    retry_delay: int = field(default=QueueConfig.DEFAULT_RETRY_DELAY, compare=False)
    
    # メタデータ
    metadata: Dict[str, Any] = field(default_factory=dict, compare=False)
    
    def __post_init__(self):
        """初期化後処理"""
        # 期限設定（作成から1時間後）
        if self.expires_at is None:
            self.expires_at = self.created_at + QueueConfig.DEFAULT_JOB_TIMEOUT
    
    def is_expired(self) -> bool:
        """期限切れ判定"""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def can_retry(self) -> bool:
        """リトライ可能か判定"""
        return self.retry_count < self.max_retries and not self.is_expired()
    
    def elapsed_seconds(self) -> float:
        """経過時間取得"""
        if self.completed_at:
            return self.completed_at - self.created_at
        elif self.started_at:
            return time.time() - self.started_at
        return time.time() - self.created_at
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書変換"""
        data = asdict(self)
        data["status"] = self.status.name
        data["elapsed"] = self.elapsed_seconds()
        data["is_expired"] = self.is_expired()
        data["can_retry"] = self.can_retry()
        return data


# ==================== メインクラス ====================
class JobQueue:
    """
    優先度付きジョブキュー管理（究極版）
    - 完全なエラーハンドリング
    - 詳細なステータス管理
    - 拡張可能な設計
    """
    
    def __init__(
        self,
        max_concurrent: int = QueueConfig.DEFAULT_MAX_CONCURRENT,
        max_queue_size: int = QueueConfig.DEFAULT_QUEUE_SIZE_LIMIT
    ):
        """
        初期化
        
        Args:
            max_concurrent: 最大同時実行数
            max_queue_size: キューサイズ上限
        """
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        
        # キュー管理
        self.queue: List[RecordingJob] = []
        self.active_jobs: Dict[str, RecordingJob] = {}
        self.completed_jobs: Dict[str, RecordingJob] = {}
        self.failed_jobs: Dict[str, RecordingJob] = {}
        
        # 統計情報
        self.stats = {
            "total_added": 0,
            "total_completed": 0,
            "total_failed": 0,
            "total_retried": 0,
            "total_expired": 0,
            "total_cancelled": 0
        }
        
        # 制御フラグ
        self.is_running = False
        self.is_shutting_down = False
        
        # ワーカー管理
        self.worker_tasks: List[asyncio.Task] = []
        self._lock = asyncio.Lock()
        
        # RecorderWrapper設定
        RecorderWrapper.configure(max_concurrent=max_concurrent)
        
        logger.info(f"JobQueue initialized (concurrent={max_concurrent}, max_size={max_queue_size})")
    
    async def add_job(
        self,
        target: str,
        duration: Optional[int] = None,
        priority: JobPriority = JobPriority.NORMAL,
        job_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        expires_in: Optional[int] = None
    ) -> Tuple[bool, str]:
        """
        ジョブ追加
        
        Args:
            target: ターゲット識別子
            duration: 録画時間
            priority: 優先度
            job_id: ジョブID
            metadata: メタデータ
            expires_in: 有効期限（秒）
            
        Returns:
            (成功フラグ, ジョブIDまたはエラーメッセージ)
        """
        # キューサイズチェック
        async with self._lock:
            if len(self.queue) >= self.max_queue_size:
                logger.warning(f"Queue is full ({self.max_queue_size})")
                return False, "queue_full"
        
        # ジョブID生成
        if not job_id:
            safe_target = target.replace(":", "_").replace("/", "_")
            job_id = f"{safe_target}_{int(time.time() * 1000)}"
        
        # 期限計算
        expires_at = None
        if expires_in:
            expires_at = time.time() + expires_in
        
        # ジョブ作成
        job = RecordingJob(
            priority=priority.value,
            job_id=job_id,
            target=target,
            duration=duration,
            metadata=metadata or {},
            expires_at=expires_at
        )
        
        # キューに追加
        async with self._lock:
            heapq.heappush(self.queue, job)
            self.stats["total_added"] += 1
        
        logger.info(f"Job added: {job_id} (priority={priority.name}, queue_size={len(self.queue)})")
        return True, job_id
    
    async def cancel_job(self, job_id: str) -> bool:
        """
        ジョブキャンセル
        
        Args:
            job_id: ジョブID
            
        Returns:
            キャンセル成功フラグ
        """
        async with self._lock:
            # キュー内を検索
            for i, job in enumerate(self.queue):
                if job.job_id == job_id:
                    job.status = JobStatus.CANCELLED
                    del self.queue[i]
                    heapq.heapify(self.queue)
                    self.stats["total_cancelled"] += 1
                    logger.info(f"Job cancelled: {job_id}")
                    return True
            
            # アクティブジョブをチェック
            if job_id in self.active_jobs:
                self.active_jobs[job_id].status = JobStatus.CANCELLED
                logger.info(f"Active job marked for cancellation: {job_id}")
                return True
        
        return False
    
    async def _process_job(self, job: RecordingJob) -> None:
        """ジョブ処理（詳細版）"""
        job.started_at = time.time()
        job.status = JobStatus.RUNNING
        
        async with self._lock:
            self.active_jobs[job.job_id] = job
        
        try:
            logger.info(f"Processing job: {job.job_id} (retry={job.retry_count})")
            
            # RecorderWrapper経由で録画
            result = await RecorderWrapper.start_record(
                job.target,
                duration=job.duration,
                job_id=job.job_id,
                metadata=job.metadata
            )
            
            job.result = result
            job.completed_at = time.time()
            
            if result.get("ok"):
                # 成功
                job.status = JobStatus.COMPLETED
                async with self._lock:
                    self.completed_jobs[job.job_id] = job
                    self.stats["total_completed"] += 1
                
                logger.info(f"Job completed: {job.job_id} ({job.elapsed_seconds():.1f}s)")
                
            else:
                # 失敗
                job.status = JobStatus.FAILED
                job.error = result.get("reason", "unknown_error")
                
                # リトライ判定
                if job.can_retry():
                    job.retry_count += 1
                    job.status = JobStatus.RETRYING
                    
                    # エクスポネンシャルバックオフ
                    delay = job.retry_delay * (2 ** (job.retry_count - 1))
                    await asyncio.sleep(min(delay, 60))
                    
                    # キューに再追加（優先度を下げて）
                    job.priority = JobPriority.LOW.value
                    job.started_at = None
                    job.status = JobStatus.PENDING
                    
                    async with self._lock:
                        heapq.heappush(self.queue, job)
                        self.stats["total_retried"] += 1
                    
                    logger.info(f"Job scheduled for retry: {job.job_id} ({job.retry_count}/{job.max_retries})")
                    
                else:
                    # リトライ不可
                    async with self._lock:
                        self.failed_jobs[job.job_id] = job
                        self.stats["total_failed"] += 1
                    
                    logger.warning(f"Job failed: {job.job_id} - {job.error}")
                    
        except asyncio.CancelledError:
            job.status = JobStatus.CANCELLED
            async with self._lock:
                self.stats["total_cancelled"] += 1
            logger.info(f"Job cancelled during processing: {job.job_id}")
            raise
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.error = str(e)
            job.result = {"error": str(e), "type": type(e).__name__}
            
            async with self._lock:
                self.failed_jobs[job.job_id] = job
                self.stats["total_failed"] += 1
            
            logger.error(f"Job error: {job.job_id} - {e}")
            
        finally:
            async with self._lock:
                self.active_jobs.pop(job.job_id, None)
    
    async def _cleanup_expired(self) -> None:
        """期限切れジョブのクリーンアップ"""
        async with self._lock:
            # 期限切れジョブを抽出
            expired = []
            remaining = []
            
            for job in self.queue:
                if job.is_expired():
                    job.status = JobStatus.EXPIRED
                    expired.append(job)
                    self.stats["total_expired"] += 1
                else:
                    remaining.append(job)
            
            if expired:
                # キューを再構築
                self.queue = remaining
                heapq.heapify(self.queue)
                
                # 失敗ジョブとして記録
                for job in expired:
                    self.failed_jobs[job.job_id] = job
                
                logger.info(f"Cleaned up {len(expired)} expired jobs")
    
    async def _worker(self, worker_id: int) -> None:
        """ワーカープロセス（改善版）"""
        logger.info(f"Worker {worker_id} started")
        
        while self.is_running and not self.is_shutting_down:
            job = None
            
            try:
                # 期限切れクリーンアップ（定期的に）
                if worker_id == 0:  # 最初のワーカーのみ
                    await self._cleanup_expired()
                
                # ジョブ取得
                async with self._lock:
                    if self.queue:
                        job = heapq.heappop(self.queue)
                
                if job:
                    # キャンセル済みチェック
                    if job.status == JobStatus.CANCELLED:
                        logger.debug(f"Skipping cancelled job: {job.job_id}")
                        continue
                    
                    # 期限切れチェック
                    if job.is_expired():
                        job.status = JobStatus.EXPIRED
                        async with self._lock:
                            self.failed_jobs[job.job_id] = job
                            self.stats["total_expired"] += 1
                        logger.info(f"Job expired: {job.job_id}")
                        continue
                    
                    logger.debug(f"Worker {worker_id} processing: {job.job_id}")
                    await self._process_job(job)
                    
                else:
                    # キューが空の場合は少し待機
                    await asyncio.sleep(QueueConfig.DEFAULT_WORKER_SLEEP)
                    
            except asyncio.CancelledError:
                logger.info(f"Worker {worker_id} cancelled")
                raise
                
            except Exception as e:
                logger.error(f"Worker {worker_id} error: {e}")
                await asyncio.sleep(QueueConfig.DEFAULT_WORKER_SLEEP)
        
        logger.info(f"Worker {worker_id} stopped")
    
    async def start(self) -> None:
        """キュー処理開始"""
        if self.is_running:
            logger.warning("Queue already running")
            return
        
        self.is_running = True
        self.is_shutting_down = False
        
        # ワーカー起動
        for i in range(self.max_concurrent):
            task = asyncio.create_task(self._worker(i))
            self.worker_tasks.append(task)
        
        logger.info(f"Queue started with {self.max_concurrent} workers")
    
    async def stop(self, timeout: float = 30.0) -> None:
        """
        キュー処理停止
        
        Args:
            timeout: 停止タイムアウト（秒）
        """
        if not self.is_running:
            logger.warning("Queue not running")
            return
        
        logger.info("Stopping queue...")
        self.is_shutting_down = True
        self.is_running = False
        
        # ワーカー停止待機
        if self.worker_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.worker_tasks, return_exceptions=True),
                    timeout=timeout
                )
            except asyncio.TimeoutError:
                logger.warning("Worker shutdown timeout, forcing cancel...")
                for task in self.worker_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            
            self.worker_tasks.clear()
        
        # RecorderWrapperシャットダウン
        await RecorderWrapper.shutdown()
        
        logger.info("Queue stopped")
    
    def get_status(self) -> Dict[str, Any]:
        """ステータス取得（詳細版）"""
        # 優先度別カウント
        priority_counts = {}
        for job in self.queue:
            priority_name = JobPriority(job.priority).name
            priority_counts[priority_name] = priority_counts.get(priority_name, 0) + 1
        
        return {
            "is_running": self.is_running,
            "is_shutting_down": self.is_shutting_down,
            "workers": len(self.worker_tasks),
            "max_concurrent": self.max_concurrent,
            "queue": {
                "size": len(self.queue),
                "max_size": self.max_queue_size,
                "by_priority": priority_counts
            },
            "active_jobs": len(self.active_jobs),
            "completed_jobs": len(self.completed_jobs),
            "failed_jobs": len(self.failed_jobs),
            "statistics": self.stats
        }
    
    def get_job_info(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        ジョブ情報取得
        
        Args:
            job_id: ジョブID
            
        Returns:
            ジョブ情報または None
        """
        # アクティブジョブ
        if job_id in self.active_jobs:
            return self.active_jobs[job_id].to_dict()
        
        # 完了ジョブ
        if job_id in self.completed_jobs:
            return self.completed_jobs[job_id].to_dict()
        
        # 失敗ジョブ
        if job_id in self.failed_jobs:
            return self.failed_jobs[job_id].to_dict()
        
        # キュー内
        for job in self.queue:
            if job.job_id == job_id:
                return job.to_dict()
        
        return None
    
    async def clear_completed(self, older_than: int = 3600) -> int:
        """
        完了ジョブのクリア
        
        Args:
            older_than: この秒数より古いジョブをクリア
            
        Returns:
            クリアされたジョブ数
        """
        cutoff = time.time() - older_than
        cleared = 0
        
        async with self._lock:
            to_remove = []
            for job_id, job in self.completed_jobs.items():
                if job.completed_at and job.completed_at < cutoff:
                    to_remove.append(job_id)
            
            for job_id in to_remove:
                del self.completed_jobs[job_id]
                cleared += 1
        
        if cleared > 0:
            logger.info(f"Cleared {cleared} old completed jobs")
        
        return cleared


# ==================== テスト用 ====================
async def main():
    """メインテスト関数"""
    print("=== JobQueue Test (v2.0 Ultimate Edition) ===")
    
    # キュー作成
    queue = JobQueue(max_concurrent=2)
    
    # ジョブ追加
    success1, job_id1 = await queue.add_job(
        "c:teruto_nico",
        duration=10,
        priority=JobPriority.HIGH,
        metadata={"test": True}
    )
    print(f"Added job 1: {success1}, {job_id1}")
    
    success2, job_id2 = await queue.add_job(
        "g:testgroup",
        duration=10,
        priority=JobPriority.NORMAL,
        expires_in=60  # 60秒で期限切れ
    )
    print(f"Added job 2: {success2}, {job_id2}")
    
    # キュー開始
    await queue.start()
    
    # ステータス表示（定期的に）
    for i in range(3):
        await asyncio.sleep(5)
        status = queue.get_status()
        print(f"\n--- Status at {i*5}s ---")
        print(json.dumps(status, indent=2))
        
        # ジョブ情報取得
        if job_id1:
            info = queue.get_job_info(job_id1)
            if info:
                print(f"\nJob {job_id1}: {info.get('status')}")
    
    # 停止
    await queue.stop()
    
    # 最終ステータス
    print("\n--- Final Status ---")
    print(json.dumps(queue.get_status(), indent=2))


if __name__ == "__main__":
    asyncio.run(main())