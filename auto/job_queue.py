#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Job Queue for TwitCasting Auto Recording (5 Workers Version)
- 同時実行5本対応
- 20人監視対応
- 優先度管理機能付き
"""
import asyncio
import json
import sys
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
import heapq

# 親ディレクトリをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

# ログディレクトリ
LOGS_DIR = Path(__file__).parent.parent / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)


class JobStatus(Enum):
    """ジョブ状態"""
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class RecordJob:
    """録画ジョブ（優先度付き）"""
    job_id: str
    target: str
    priority: int  # 1が最優先
    created_at: float
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    duration_sec: Optional[float] = None
    worker_id: Optional[str] = None
    result: dict = field(default_factory=dict)
    
    def __lt__(self, other):
        """優先度比較（heapq用）"""
        return self.priority < other.priority
    
    def to_dict(self) -> dict:
        """辞書変換"""
        return {
            "job_id": self.job_id,
            "target": self.target,
            "priority": self.priority,
            "status": self.status.value,
            "worker_id": self.worker_id,
            "created_at": datetime.fromtimestamp(self.created_at).strftime("%Y-%m-%d %H:%M:%S"),
            "duration_sec": self.duration_sec
        }


class PriorityQueue:
    """優先度付きキュー"""
    
    def __init__(self, maxsize: int = 100):
        self.queue: List[RecordJob] = []
        self.maxsize = maxsize
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Condition()
        
    async def put(self, job: RecordJob):
        """ジョブ追加"""
        async with self._lock:
            if len(self.queue) >= self.maxsize:
                raise asyncio.QueueFull()
            heapq.heappush(self.queue, job)
            
        async with self._not_empty:
            self._not_empty.notify()
            
    async def get(self) -> RecordJob:
        """優先度順でジョブ取得"""
        async with self._not_empty:
            while not self.queue:
                await self._not_empty.wait()
                
        async with self._lock:
            return heapq.heappop(self.queue)
            
    def qsize(self) -> int:
        """キューサイズ"""
        return len(self.queue)
        
    def empty(self) -> bool:
        """空チェック"""
        return len(self.queue) == 0


class JobQueue:
    """マルチワーカー対応ジョブキュー"""
    
    def __init__(
        self,
        max_workers: int = 1,
        max_targets: int = 20,
        cooldown_sec: int = 60,
        max_queue_size: int = 100
    ):
        """
        初期化
        
        Args:
            max_workers: 最大同時実行数（5）
            max_targets: 最大監視対象数（20）
            cooldown_sec: クールダウン秒数
            max_queue_size: キュー最大サイズ
        """
        # 基本設定
        self.max_workers = max_workers
        self.max_targets = max_targets
        self.cooldown_sec = cooldown_sec
        
        # 優先度付きキュー
        self.queue = PriorityQueue(maxsize=max_queue_size)
        
        # 状態管理
        self.cooldown_map: Dict[str, float] = {}
        self.running_jobs: Dict[str, RecordJob] = {}  # worker_id -> job
        self.processed_jobs: List[RecordJob] = []
        self.monitored_targets: Set[str] = set()
        
        # 重複検出
        self._pending_targets: Set[str] = set()
        self._running_targets: Set[str] = set()
        
        # ワーカー管理
        self.worker_tasks: List[asyncio.Task] = []
        self._running = False
        self._state_lock = asyncio.Lock()
        
        # 統計
        self.stats = {
            "total_queued": 0,
            "total_completed": 0,
            "total_failed": 0,
            "by_worker": {f"worker_{i}": 0 for i in range(max_workers)}
        }
        
        # ログ
        self.log_file = LOGS_DIR / f"queue_{datetime.now():%Y%m%d}.jsonl"
        
    async def start(self):
        """全ワーカー起動"""
        if self._running:
            self._log_event("already_running", {})
            return
            
        self._running = True
        
        # 5つのワーカーを起動
        self.worker_tasks = [
            asyncio.create_task(
                self._run_worker(f"worker_{i}"),
                name=f"worker_{i}"
            )
            for i in range(self.max_workers)
        ]
        
        self._log_event("queue_started", {
            "max_workers": self.max_workers,
            "max_targets": self.max_targets,
            "cooldown_sec": self.cooldown_sec
        })
        print(f"[QUEUE] Started with {self.max_workers} workers")
        
    async def stop(self):
        """全ワーカー停止"""
        self._running = False
        
        # すべてのワーカーをキャンセル
        for task in self.worker_tasks:
            task.cancel()
            
        if self.worker_tasks:
            await asyncio.gather(*self.worker_tasks, return_exceptions=True)
            
        self._log_event("queue_stopped", {"stats": self.stats})
        print(f"[QUEUE] Stopped (completed: {self.stats['total_completed']})")
        
    async def enqueue(
        self,
        target: str,
        priority: int = 10,
        meta: Optional[dict] = None
    ) -> dict:
        """
        ジョブ追加（優先度付き）
        
        Args:
            target: "c:username" or "g:groupid"
            priority: 優先度（1が最高、20が最低）
            meta: メタデータ
            
        Returns:
            {"status": str, "job_id": str|None, ...}
        """
        # バリデーション
        if not self._validate_target(target):
            self._log_event("invalid_target", {"target": target})
            return {"status": "invalid", "job_id": None}
            
        # 監視対象数チェック
        if len(self.monitored_targets) >= self.max_targets:
            if target not in self.monitored_targets:
                self._log_event("max_targets_reached", {
                    "target": target,
                    "current": len(self.monitored_targets)
                })
                return {"status": "max_targets_reached", "job_id": None}
                
        async with self._state_lock:
            # 監視対象に追加
            self.monitored_targets.add(target)
            
            # クールダウンチェック
            if self._is_cooldown(target):
                remaining = self._get_cooldown_remaining(target)
                self._log_event("cooldown", {
                    "target": target,
                    "remaining_sec": remaining
                })
                return {
                    "status": "cooldown",
                    "job_id": None,
                    "remaining_sec": remaining
                }
                
            # 重複チェック（待機中）
            if target in self._pending_targets:
                self._log_event("already_queued", {"target": target})
                return {"status": "already_queued", "job_id": None}
                
            # 重複チェック（実行中）
            if target in self._running_targets:
                # 実行中のジョブを探す
                running_job = None
                for job in self.running_jobs.values():
                    if job and job.target == target:
                        running_job = job
                        break
                        
                self._log_event("already_running", {
                    "target": target,
                    "job_id": running_job.job_id if running_job else "unknown"
                })
                return {
                    "status": "already_running",
                    "job_id": running_job.job_id if running_job else None
                }
                
            # ジョブ作成
            job_id = f"{target.replace(':', '_')}_{int(time.time()*1000)}"
            self._pending_targets.add(target)
            
        # ジョブ作成（ロック外）
        job = RecordJob(
            job_id=job_id,
            target=target,
            priority=priority,
            created_at=time.time()
        )
        
        # キュー投入
        try:
            await self.queue.put(job)
        except asyncio.QueueFull:
            async with self._state_lock:
                self._pending_targets.discard(target)
            self._log_event("queue_full", {"target": target})
            return {"status": "queue_full", "job_id": None}
            
        # 成功
        self.stats["total_queued"] += 1
        self._log_event("job_queued", {
            "job_id": job_id,
            "target": target,
            "priority": priority,
            "queue_size": self.queue.qsize()
        })
        print(f"[QUEUE] Queued: {job_id} (priority={priority}, size={self.queue.qsize()})")
        
        return {
            "status": "queued",
            "job_id": job_id,
            "priority": priority,
            "queue_size": self.queue.qsize()
        }
        
    async def _run_worker(self, worker_id: str):
        """ワーカーループ"""
        print(f"[QUEUE] {worker_id} started")
        
        while self._running:
            try:
                # ジョブ取得（優先度順）
                job = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=1.0
                )
                
                # 状態更新
                async with self._state_lock:
                    self._pending_targets.discard(job.target)
                    self._running_targets.add(job.target)
                    self.running_jobs[worker_id] = job
                    job.worker_id = worker_id
                    
                # 実行
                await self._execute_job(job, worker_id)
                
                # 統計更新
                self.stats["by_worker"][worker_id] += 1
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._log_event("worker_error", {
                    "worker_id": worker_id,
                    "error": str(e)
                })
                print(f"[QUEUE] {worker_id} error: {e}")
                await asyncio.sleep(5)
                
        print(f"[QUEUE] {worker_id} stopped")
        
    async def _execute_job(self, job: RecordJob, worker_id: str):
        """ジョブ実行"""
        try:
            # 開始
            job.status = JobStatus.RUNNING
            job.started_at = time.time()
            
            self._log_event("job_start", {
                "job_id": job.job_id,
                "target": job.target,
                "worker_id": worker_id,
                "priority": job.priority
            })
            print(f"[QUEUE] {worker_id} executing: {job.job_id}")
            
            # 録画実行（Cookie分離）
            result = await self._call_recorder(job.target, job.job_id)
            
            # 結果処理
            job.finished_at = time.time()
            job.duration_sec = job.finished_at - job.started_at
            job.result = result
            
            if result.get("ok", False):
                job.status = JobStatus.DONE
                self.stats["total_completed"] += 1
                
                # クールダウン設定
                async with self._state_lock:
                    self.cooldown_map[job.target] = job.finished_at
                    self._cleanup_cooldown_map()
                    
                self._log_event("job_done", {
                    "job_id": job.job_id,
                    "worker_id": worker_id,
                    "duration_sec": job.duration_sec
                })
                print(f"[QUEUE] {worker_id} completed: {job.job_id} ({job.duration_sec:.1f}s)")
                
            else:
                job.status = JobStatus.FAILED
                self.stats["total_failed"] += 1
                
                self._log_event("job_failed", {
                    "job_id": job.job_id,
                    "worker_id": worker_id,
                    "reason": result.get("reason", "unknown")
                })
                print(f"[QUEUE] {worker_id} failed: {job.job_id}")
                
        except Exception as e:
            job.status = JobStatus.FAILED
            job.finished_at = time.time()
            self.stats["total_failed"] += 1
            
            self._log_event("job_error", {
                "job_id": job.job_id,
                "worker_id": worker_id,
                "error": str(e)
            })
            print(f"[QUEUE] {worker_id} exception: {e}")
            
        finally:
            # 終了処理
            async with self._state_lock:
                self._running_targets.discard(job.target)
                self.running_jobs[worker_id] = None
                self.processed_jobs.append(job)
                if len(self.processed_jobs) > 200:  # 5ワーカー分保持
                    self.processed_jobs = self.processed_jobs[-200:]
                    
    async def _call_recorder(self, target: str, job_id: str) -> dict:
        """録画呼び出し（Cookie分離対応）"""
        try:
            from auto.recorder_wrapper import RecorderWrapper
            
            # Cookie分離のためjob_idを渡す
            return await RecorderWrapper.start_record(
                target,
                job_id=job_id  # Cookie分離用
            )
        except ImportError:
            return {"ok": False, "reason": "import_error"}
        except Exception as e:
            return {"ok": False, "reason": f"error:{e.__class__.__name__}"}
            
    def _cleanup_cooldown_map(self):
        """古いクールダウンを削除"""
        now = time.time()
        expired = [
            k for k, v in self.cooldown_map.items()
            if now - v > self.cooldown_sec * 2
        ]
        for k in expired:
            del self.cooldown_map[k]
            
    def _validate_target(self, target: str) -> bool:
        """ターゲット検証"""
        if not isinstance(target, str) or len(target) < 3:
            return False
        return target.startswith(("c:", "g:"))
        
    def _is_cooldown(self, target: str) -> bool:
        """クールダウン判定"""
        if target not in self.cooldown_map:
            return False
        return time.time() - self.cooldown_map[target] < self.cooldown_sec
        
    def _get_cooldown_remaining(self, target: str) -> int:
        """クールダウン残り秒数"""
        if target not in self.cooldown_map:
            return 0
        elapsed = time.time() - self.cooldown_map[target]
        return max(0, int(self.cooldown_sec - elapsed))
        
    def _log_event(self, event: str, data: dict):
        """ログ出力"""
        try:
            entry = {
                "t": datetime.now().isoformat(),
                "event": event,
                **data
            }
            with open(self.log_file, "a", encoding="utf-8") as f:
                json.dump(entry, f, ensure_ascii=False)
                f.write("\n")
        except Exception:
            pass
            
    def get_status(self) -> dict:
        """ステータス取得"""
        return {
            "running": self._running,
            "workers": self.max_workers,
            "queue_size": self.queue.qsize(),
            "running_jobs": {
                worker_id: job.to_dict() if job else None
                for worker_id, job in self.running_jobs.items()
            },
            "pending_targets": list(self._pending_targets),
            "running_targets": list(self._running_targets),
            "cooldown_targets": list(self.cooldown_map.keys()),
            "monitored_targets": list(self.monitored_targets),
            "stats": self.stats
        }


if __name__ == "__main__":
    async def test():
        # 5ワーカーでテスト
        queue = JobQueue(max_workers=5, max_targets=20)
        await queue.start()
        
        # 優先度付きでジョブ追加
        targets = [
            ("c:teruto_nico", 1),  # 最優先
            ("c:user2", 5),
            ("c:user3", 10),
            ("c:user4", 15),
            ("c:user5", 20),  # 最低優先
        ]
        
        for target, priority in targets:
            result = await queue.enqueue(target, priority=priority)
            print(f"Enqueue {target}: {result}")
            
        # ステータス確認
        print(f"Status: {json.dumps(queue.get_status(), indent=2)}")
        
        await asyncio.sleep(10)
        await queue.stop()
        
    asyncio.run(test())