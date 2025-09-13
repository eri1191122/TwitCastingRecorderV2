#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Recorder Wrapper for TwitCasting Auto Recording
Version: 4.8.3 (完全修正版・型安全性強化・セマフォ確実解放)
- フェーズ別のタイムアウト管理（HLS取得150秒、録画中はファイルサイズで判定）
- タスク破棄エラーの完全抑制
- 機能削減なし、全機能維持
- 【修正】job未初期化参照の完全防止
- 【修正】Cookie更新I/F実装
- 【修正】タイムアウト延長
- 【修正】公開set_stateメソッド追加
- 【修正】型安全性強化（float.cancel()エラー解消）
- 【修正】セマフォ解放の確実化
"""

from __future__ import annotations
from pathlib import Path

import builtins as _bi
if not hasattr(_bi, "Path"):
    _bi.Path = Path

import asyncio
import json
import sys
import time
import traceback
import threading
import gc
import os
import contextlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple
from weakref import WeakKeyDictionary
from collections import defaultdict

import logging

logger = logging.getLogger("wrapper")
logger.setLevel(logging.INFO)
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
if not logger.handlers:
    logger.addHandler(_ch)

# ==================== ルート解決 ====================

def _get_project_root() -> Path:
    """プロジェクトルートを堅牢に解決"""
    here = Path(__file__).resolve()
    candidate = here.parents[1]
    for p in [candidate, *here.parents[2:5]]:
        if (p / "facade.py").exists() or (p / "tc_recorder_core.py").exists():
            return p
    return candidate

PROJECT_ROOT = _get_project_root()
BASE_DIR = PROJECT_ROOT
LOGS_DIR = PROJECT_ROOT / "logs"
RECORDINGS_DIR = PROJECT_ROOT / "recordings"
COOKIES_DIR = LOGS_DIR

for d in (LOGS_DIR, RECORDINGS_DIR, COOKIES_DIR):
    try:
        d.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Failed to ensure dir {d}: {e}")

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ==================== 設定クラス ====================

@dataclass
class WrapperConfig:
    max_concurrent: int = 1
    LOG_ROTATE_SIZE_MB: int = 100
    LOG_KEEP_DAYS: int = 30
    LOGIN_CHECK_INTERVAL: int = 300
    SEMAPHORE_ACQUIRE_TIMEOUT: float = 30.0
    HLS_ACQUISITION_TIMEOUT: int = 150  # HLS取得フェーズのタイムアウト（90→150秒に延長）
    FILE_STALL_TIMEOUT: int = 45  # ファイルサイズ変化なしの許容時間（30→45秒に延長）
    ABSOLUTE_RECORDING_TIMEOUT: int = 3600
    FILE_CHECK_INTERVAL: int = 5  # ファイルサイズチェック間隔

# ==================== Enum定義 ====================

class TargetPrefix(Enum):
    URL = "url"
    C = "c"
    G = "g"
    IG = "ig"
    F = "f"
    TW = "tw"

class RecordingStatus(Enum):
    IDLE = auto()
    PREPARING = auto()
    LOGIN_CHECK = auto()
    RECORDING = auto()
    FINALIZING = auto()
    COMPLETED = auto()
    ERROR = auto()
    CANCELLED = auto()
    TIMEOUT = auto()
    DEADLOCK = auto()

    def is_active(self) -> bool:
        return self in {
            RecordingStatus.PREPARING,
            RecordingStatus.LOGIN_CHECK,
            RecordingStatus.RECORDING,
            RecordingStatus.FINALIZING,
        }

class RecordingPhase(Enum):
    """録画フェーズの定義"""
    STARTING = "starting"  # HLS取得中
    RECORDING = "recording"  # 実際に録画中
    STOPPING = "stopping"  # 停止処理中
    IDLE = "idle"  # 待機中
    ERROR = "error"  # エラー状態
    WAITING = "waiting"  # 容量待ち

@dataclass
class RecordingJob:
    job_id: str
    target: str
    url: str
    status: RecordingStatus = RecordingStatus.IDLE
    phase: RecordingPhase = RecordingPhase.IDLE
    started_at: float = field(default_factory=time.time)
    completed_at: Optional[float] = None
    duration: Optional[int] = None
    output_files: List[str] = field(default_factory=list)
    error: Optional[str] = None
    raw_result: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    semaphore_acquired: bool = False
    proc_gate_acquired: bool = False
    url_lock_acquired: bool = False
    deadlock_timer: Optional[asyncio.Task] = None
    file_monitor_task: Optional[asyncio.Task] = None
    last_file_size: int = 0
    last_file_check: float = field(default_factory=time.time)

# ==================== GUI同期用関数 ====================

def _emit_gui_state(recording: bool, url: str, job_id: str = "", ok: Optional[bool] = None) -> None:
    """GUIがtailするJSONL（logs/monitor_gui_bridge.jsonl）へ状態を1行追記"""
    try:
        bridge = PROJECT_ROOT / "logs" / "monitor_gui_bridge.jsonl"
        bridge.parent.mkdir(parents=True, exist_ok=True)
        line = {
            "ts": int(time.time()),
            "type": "GUI-STATE",
            "recording": bool(recording),
            "url": url,
            "job_id": job_id
        }
        if ok is not None:
            line["ok"] = bool(ok)
        with bridge.open("a", encoding="utf-8") as f:
            f.write(json.dumps(line, ensure_ascii=False) + "\n")
        logger.debug(f"GUI-STATE emitted: {line}")
    except Exception as e:
        logger.debug(f"GUI-STATE emit skipped: {e}")

# ==================== RecorderWrapper 本体 ====================

class RecorderWrapper:
    _configured: bool = False
    _is_initialized: bool = False
    _config = WrapperConfig()
    _last_configure_ts: float = 0

    _loop_semaphores: "WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = WeakKeyDictionary()
    _loop_semaphores_lock = threading.Lock()

    _proc_gate = threading.Semaphore(1)
    _proc_gate_reset_lock = threading.Lock()
    _proc_gate_stale_count = 0
    
    _recording_locks: Dict[str, threading.Lock] = {}
    _global_state_lock = threading.RLock()
    
    _shutdown_event = threading.Event()

    _recorder_instance: Any = None
    _recorder_init_lock = threading.Lock()

    _recording_jobs: Dict[str, RecordingJob] = {}
    _last_login_check: Optional[float] = None
    
    _total_recordings = 0
    _total_successes = 0
    _total_failures = 0
    _last_activity_time = time.time()
    
    # ===== 録画状態の真実の源 =====
    _recording_states: Dict[str, str] = defaultdict(lambda: "idle")
    _recording_phases: Dict[str, RecordingPhase] = defaultdict(lambda: RecordingPhase.IDLE)
    _states_lock = threading.RLock()

    # ==================== 状態管理 ====================
    
    @classmethod
    def get_recording_states(cls) -> Dict[str, str]:
        """GUIから安全に参照できる状態のコピーを返す"""
        with cls._states_lock:
            return dict(cls._recording_states)
    
    @classmethod
    def _set_state(cls, url: str, state: str) -> None:
        """内部用：状態更新とログ記録"""
        with cls._states_lock:
            old = cls._recording_states.get(url, "idle")
            cls._recording_states[url] = state
            cls._log_event("state_transition", {
                "url": url,
                "from": old,
                "to": state,
                "timestamp": datetime.now().isoformat()
            })
    
    @classmethod
    def set_state(cls, url: str, state: str) -> None:
        """外部からの状態設定（Engine用）- 公開I/F"""
        with cls._states_lock:
            cls._set_state(url, state)
    
    @classmethod
    def _set_phase(cls, url: str, phase: RecordingPhase) -> None:
        """録画フェーズの更新"""
        with cls._states_lock:
            old = cls._recording_phases.get(url, RecordingPhase.IDLE)
            cls._recording_phases[url] = phase
            logger.debug(f"Phase transition for {url}: {old.value} -> {phase.value}")

    # ==================== Cookie更新メソッド（新規追加） ====================
    
    @classmethod
    async def ensure_complete_cookies(cls, force_refresh: bool = False) -> bool:
        """GUI Cookie更新ボタン用（ChromeSingleton経由）"""
        try:
            rec = await cls._ensure_recorder()
            
            # 強制更新時は再ログイン
            if force_refresh:
                success = await cls.ensure_login(force=True)
                if not success:
                    cls._log_event("cookie_refresh_failed", {"reason": "login_failed"})
                    return False
            
            # Cookie出力
            if hasattr(rec, 'chrome') and hasattr(rec.chrome, 'export_cookies'):
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                out = COOKIES_DIR / f"cookies_update_{ts}.txt"
                
                # ChromeSingletonのexport_cookies使用
                success = await rec.chrome.export_cookies(out)
                
                cls._log_event("cookie_manual_update", {
                    "path": str(out),
                    "success": success,
                    "force_refresh": force_refresh
                })
                return success
            else:
                # 代替実装：tc_recorder_coreから直接
                try:
                    from tc_recorder_core import _save_cookies_netscape
                    ctx = await rec.chrome.ensure_headless()
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    out = COOKIES_DIR / f"cookies_update_{ts}.txt"
                    await _save_cookies_netscape(ctx, out, ".twitcasting.tv")
                    
                    cls._log_event("cookie_manual_update", {
                        "path": str(out),
                        "success": True,
                        "force_refresh": force_refresh
                    })
                    return True
                except Exception as e:
                    logger.error(f"Cookie export via core failed: {e}")
                    cls._log_event("cookie_update_error", {"error": str(e)})
                    return False
                
        except Exception as e:
            logger.error(f"Cookie update error: {e}")
            cls._log_event("cookie_update_error", {"error": str(e)})
            return False

    # ==================== ログ出力 ====================
    
    @classmethod
    def _resolve_log_path(cls) -> Path:
        """wrapper_YYYYMMDD_NNN.jsonl を返す"""
        date = datetime.now().strftime("%Y%m%d")
        base_glob = LOGS_DIR.glob(f"wrapper_{date}_*.jsonl")
        max_idx = 0
        for p in base_glob:
            try:
                idx = int(p.stem.split("_")[-1])
                max_idx = max(max_idx, idx)
            except Exception:
                continue
        path = LOGS_DIR / f"wrapper_{date}_{max(1, max_idx):03d}.jsonl"
        try:
            if path.exists() and path.stat().st_size >= cls._config.LOG_ROTATE_SIZE_MB * 1024 * 1024:
                path = LOGS_DIR / f"wrapper_{date}_{max_idx+1:03d}.jsonl"
        except Exception:
            pass
        return path

    @classmethod
    def _log_event(cls, event: str, payload: Optional[Dict[str, Any]] = None) -> None:
        payload = payload or {}
        try:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            p = cls._resolve_log_path()
            with p.open("a", encoding="utf-8") as f:
                f.write(json.dumps({"ts": ts, "event": event, **payload}, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.warning(f"log_event failed: {e}")

    # ==================== セマフォ取得 ====================
    
    @classmethod
    def _get_loop_semaphore(cls) -> asyncio.Semaphore:
        loop = asyncio.get_event_loop()
        with cls._loop_semaphores_lock:
            sem = cls._loop_semaphores.get(loop)
            if sem is None:
                sem = asyncio.Semaphore(max(1, int(cls._config.max_concurrent or 1)))
                cls._loop_semaphores[loop] = sem
            return sem

    # ==================== 設定/初期化 ====================
    
    @classmethod
    def configure(cls, max_concurrent: int = 1) -> None:
        cls._shutdown_event.clear()
        cls._last_configure_ts = time.monotonic()
        
        cls._config.max_concurrent = max(1, int(max_concurrent or 1))
        with cls._loop_semaphores_lock:
            for loop in list(cls._loop_semaphores.keys()):
                cls._loop_semaphores[loop] = asyncio.Semaphore(cls._config.max_concurrent)
        cls._configured = True
        logger.info(
            "RecorderWrapper configured (max_concurrent=%s, root=%s)",
            cls._config.max_concurrent, PROJECT_ROOT
        )

    @classmethod
    async def initialize(cls) -> None:
        if cls._is_initialized:
            return
        rec = await cls._ensure_recorder()
        if hasattr(rec, "initialize"):
            await rec.initialize()
        cls._is_initialized = True
        logger.info("RecorderWrapper initialized")

    # ==================== 緊急リセット ====================
    
    @classmethod
    def emergency_reset(cls) -> None:
        """緊急時の完全リセット（録画中保護）"""
        cls._shutdown_event.clear()
        logger.warning("Cleared shutdown event in emergency reset")
        
        if cls._recording_jobs:
            active_count = len(cls._recording_jobs)
            logger.warning(f"Emergency reset skipped: {active_count} active jobs")
            cls._log_event("emergency_reset_skipped", {
                "active_jobs": active_count,
                "job_ids": list(cls._recording_jobs.keys())
            })
            return
        
        logger.warning("Emergency reset initiated")
        
        with cls._proc_gate_reset_lock:
            cls._proc_gate = threading.Semaphore(cls._config.max_concurrent)
            cls._proc_gate_stale_count = 0
        
        with cls._loop_semaphores_lock:
            cls._loop_semaphores.clear()
        
        with cls._global_state_lock:
            cls._recording_locks.clear()
        
        # 状態もクリア
        with cls._states_lock:
            cls._recording_states.clear()
            cls._recording_phases.clear()
        
        cls._recording_jobs.clear()
        cls._last_activity_time = time.time()
        
        cls._log_event("emergency_reset", {
            "reason": "startup_cleanup",
            "timestamp": datetime.now().isoformat()
        })
        
        gc.collect()
        logger.warning("Emergency reset completed")

    # ==================== Recorderインスタンス確保 ====================
    
    @classmethod
    async def _ensure_recorder(cls) -> Any:
        with cls._recorder_init_lock:
            if cls._recorder_instance is None:
                try:
                    from facade import TwitCastingRecorder
                except Exception as e1:
                    try:
                        root_str = str(PROJECT_ROOT)
                        if root_str not in sys.path:
                            sys.path.insert(0, root_str)
                        from facade import TwitCastingRecorder
                    except Exception as e2:
                        try:
                            from auto.facade import TwitCastingRecorder
                        except Exception as e3:
                            logger.error(
                                "TwitCastingRecorder import failed: "
                                f"root_try={e1!r}, sys_path_try={e2!r}, auto_try={e3!r}"
                            )
                            raise
                cls._recorder_instance = TwitCastingRecorder()
        rec = cls._recorder_instance
        if getattr(rec, "_initialized", False):
            return rec
        if hasattr(rec, "initialize"):
            await rec.initialize()
        return rec

    # ==================== ログイン確保 ====================
    
    @classmethod
    async def ensure_login(cls, force: bool = False) -> bool:
        return await cls._ensure_login(force=force)

    @classmethod
    async def _ensure_login(cls, force: bool = False) -> bool:
        try:
            recorder = await cls._ensure_recorder()
        except Exception as e:
            logger.error(f"Failed to get recorder: {e}")
            return False

        try:
            status = None
            if hasattr(recorder, "test_login_status"):
                status = await recorder.test_login_status()
                cls._log_event("login_check", {"status": status})
            else:
                logger.warning("test_login_status not found; assuming logged in")
                return True

            must_open = bool(force) or not (status in ("strong", True))
            if not hasattr(recorder, "setup_login"):
                logger.error("setup_login not found")
                return False

            if must_open:
                logger.info("[login] Opening login wizard (force=%s, current=%s)", force, status)
                cls._log_event("login_open_wizard", {"current_status": status, "force": force})
                success = await recorder.setup_login()
            else:
                cls._last_login_check = time.time()
                return True

            if success:
                logger.info("✅ Login successful!")
                cls._last_login_check = time.time()
                cls._log_event("login_success", {})
                
                try:
                    from tc_recorder_core import _save_cookies_netscape
                    ctx = await recorder.chrome.ensure_headless()
                    
                    session_found = False
                    max_retries = 10
                    
                    for i in range(max_retries):
                        cookies = await ctx.cookies()
                        session_found = any(c["name"] == "_twitcasting_session" for c in cookies)
                        
                        if session_found:
                            logger.info(f"✅ _twitcasting_session found after {i*0.5}s")
                            break
                        
                        if i < max_retries - 1:
                            logger.info(f"⚠️ _twitcasting_session not found yet, retry {i+1}/{max_retries}")
                            await asyncio.sleep(0.5)
                    
                    if not session_found:
                        logger.info("⚠️ _twitcasting_session NOT FOUND after 5 seconds (tc_ss may be sufficient)")
                    
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    out = COOKIES_DIR / f"cookies_enter_{ts}.txt"
                    await _save_cookies_netscape(ctx, out, ".twitcasting.tv")
                    
                    with out.open("r", encoding="utf-8") as f:
                        content = f.read()
                    
                    has_legacy = "_twitcasting_session" in content
                    has_tc_ss = "tc_ss" in content
                    
                    if has_legacy or has_tc_ss:
                        logger.info(f"✅ Cookie ok for login (legacy={has_legacy}, tc_ss={has_tc_ss}): {out}")
                    else:
                        logger.warning(f"⚠️ Cookie may be insufficient (no legacy/tc_ss): {out}")
                    
                    cls._log_event("cookie_exported", {
                        "path": str(out), 
                        "has_legacy": has_legacy,
                        "has_tc_ss": has_tc_ss
                    })
                    
                    latest = COOKIES_DIR / "latest_cookie_path.txt"
                    with latest.open("w", encoding="utf-8") as f:
                        f.write(str(out))
                    cls._log_event("cookie_latest_path_saved", {"path": str(latest)})
                    
                except Exception as ce:
                    logger.warning(f"Cookie export failed (non-fatal): {ce}")
                
                return True

            logger.error("❌ Login failed")
            cls._log_event("login_failed", {})
            return False

        except asyncio.CancelledError:
            raise
        except Exception as e:
            cls._log_event("login_error", {"error": str(e), "type": type(e).__name__})
            logger.error(f"Login error: {e}")
            return False

    # ==================== フェーズ別デッドロック検出（改良版） ====================
    
    @classmethod
    async def _phase_aware_deadlock_detector(cls, url: str, job_id: str) -> None:
        """フェーズ別のタイムアウト管理（修正版：型安全）"""
        try:
            job = cls._recording_jobs.get(job_id)
            if not job:
                return
            
            # Phase 1: HLS取得フェーズ（150秒待つ）
            start_time = time.time()
            while time.time() - start_time < cls._config.HLS_ACQUISITION_TIMEOUT:
                await asyncio.sleep(5)
                
                with cls._states_lock:
                    phase = cls._recording_phases.get(url, RecordingPhase.IDLE)
                    state = cls._recording_states.get(url, "idle")
                    
                    if phase != RecordingPhase.STARTING:
                        # HLS取得完了またはエラー
                        if phase == RecordingPhase.RECORDING:
                            logger.info(f"HLS captured for {url}, entering recording phase")
                            break
                        else:
                            return
                    
                    # まだHLS取得中
                    elapsed = int(time.time() - start_time)
                    if elapsed % 30 == 0:
                        logger.info(f"Still waiting for HLS for {url}: {elapsed}s")
            
            # HLS取得タイムアウト
            with cls._states_lock:
                if cls._recording_phases.get(url) == RecordingPhase.STARTING:
                    cls._log_event("hls_timeout", {
                        "url": url,
                        "job_id": job_id,
                        "timeout": cls._config.HLS_ACQUISITION_TIMEOUT
                    })
                    cls._set_state(url, "error")
                    cls._set_phase(url, RecordingPhase.ERROR)
                    logger.error(f"HLS acquisition timeout for {url} after {cls._config.HLS_ACQUISITION_TIMEOUT}s")
                    return
            
            # Phase 2: 録画フェーズ（ファイルサイズ監視）
            if job.output_files:
                # ファイルモニタータスクを起動
                job.file_monitor_task = asyncio.create_task(
                    cls._monitor_file_growth(url, job_id)
                )
                await job.file_monitor_task
            else:
                # ファイルがない場合は通常のタイムアウト監視
                max_duration = cls._config.ABSOLUTE_RECORDING_TIMEOUT
                for i in range(max_duration // 60):
                    await asyncio.sleep(60)
                    
                    with cls._states_lock:
                        phase = cls._recording_phases.get(url, RecordingPhase.IDLE)
                        if phase != RecordingPhase.RECORDING:
                            return
                    
                    logger.info(f"Recording ongoing for {url}: {i+1} minutes")
                
                # 絶対タイムアウト
                cls._log_event("recording_timeout", {
                    "url": url,
                    "job_id": job_id,
                    "duration": max_duration
                })
                cls._set_state(url, "error")
                cls._set_phase(url, RecordingPhase.ERROR)
                logger.error(f"Recording timeout for {url} after {max_duration/60} minutes")
            
        except asyncio.CancelledError:
            # 正常キャンセル（タスク破棄エラー抑制）
            pass
        except Exception as e:
            logger.error(f"Deadlock detector error: {e}")
    
    @classmethod
    async def _monitor_file_growth(cls, url: str, job_id: str) -> None:
        """ファイルサイズの成長を監視"""
        try:
            job = cls._recording_jobs.get(job_id)
            if not job or not job.output_files:
                return
            
            file_path = Path(job.output_files[0])
            if not file_path.exists():
                logger.warning(f"Output file not found: {file_path}")
                return
            
            stall_count = 0
            last_size = 0
            
            while True:
                await asyncio.sleep(cls._config.FILE_CHECK_INTERVAL)
                
                with cls._states_lock:
                    phase = cls._recording_phases.get(url, RecordingPhase.IDLE)
                    if phase != RecordingPhase.RECORDING:
                        return
                
                try:
                    current_size = file_path.stat().st_size
                    if current_size > last_size:
                        # ファイルが成長している
                        stall_count = 0
                        last_size = current_size
                        job.last_file_size = current_size
                        job.last_file_check = time.time()
                        
                        if current_size > 1024 * 1024:  # 1MB以上
                            size_mb = current_size / (1024 * 1024)
                            logger.debug(f"Recording active for {url}: {size_mb:.1f} MB")
                    else:
                        # ファイルサイズ変化なし
                        stall_count += 1
                        if stall_count * cls._config.FILE_CHECK_INTERVAL >= cls._config.FILE_STALL_TIMEOUT:
                            logger.warning(f"File growth stalled for {url} for {cls._config.FILE_STALL_TIMEOUT}s")
                            cls._log_event("file_stall_detected", {
                                "url": url,
                                "job_id": job_id,
                                "last_size": last_size,
                                "stall_duration": cls._config.FILE_STALL_TIMEOUT
                            })
                            # エラーとして扱う
                            cls._set_state(url, "error")
                            cls._set_phase(url, RecordingPhase.ERROR)
                            return
                        
                except (OSError, IOError) as e:
                    logger.warning(f"File monitoring error for {url}: {e}")
                    # ファイルアクセスエラーは無視して継続
                    
        except asyncio.CancelledError:
            # 正常キャンセル
            pass
        except Exception as e:
            logger.error(f"File monitor error: {e}")

    # ==================== 録画開始（完全版・修正版） ====================
    
    @classmethod
    async def start_record(
        cls,
        target: str,
        *,
        hint_url: Optional[str] = None,
        duration: Optional[int] = None,
        job_id: Optional[str] = None,
        force_login_check: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        job = None  # 【修正】未初期化参照防止のため最初に初期化
        start_time = time.time()
        _gui_started = False
        
        proc_gate_acquired = False
        sem_acquired = False
        url_lock_acquired = False
        deadlock_timer = None
        
        cls._last_activity_time = time.time()
        cls._total_recordings += 1

        if cls._shutdown_event.is_set():
            return cls._create_error_result(None, "shutdown_in_progress", start_time)

        if not cls._configured:
            cls.configure()

        loop_semaphore = cls._get_loop_semaphore()

        job_id = job_id or f"job_{int(start_time)}"
        url = cls._build_url(target, hint_url)

        try:
            # ===== 状態チェックと遷移 =====
            with cls._states_lock:
                current = cls._recording_states.get(url, "idle")
                if current != "idle":
                    cls._log_event("start_rejected", {
                        "url": url,
                        "current_state": current
                    })
                    return cls._create_error_result(None, f"already_{current}", start_time)
                cls._set_state(url, "starting")
                cls._set_phase(url, RecordingPhase.STARTING)
            
            # ===== URL別排他制御 =====
            with cls._global_state_lock:
                if url not in cls._recording_locks:
                    cls._recording_locks[url] = threading.Lock()
            
            url_lock = cls._recording_locks[url]
            url_lock_acquired = url_lock.acquire(blocking=False)
            
            if not url_lock_acquired:
                cls._log_event("url_already_recording", {"url": url})
                cls._set_state(url, "idle")
                cls._set_phase(url, RecordingPhase.IDLE)
                return cls._create_error_result(None, "url_already_recording", start_time)
            
            # ===== プロセス全体ゲート =====
            try:
                proc_gate_acquired = await asyncio.to_thread(
                    cls._proc_gate.acquire, True, cls._config.SEMAPHORE_ACQUIRE_TIMEOUT
                )
                if not proc_gate_acquired:
                    if not cls._recording_jobs and cls._proc_gate_stale_count < 3:
                        with cls._proc_gate_reset_lock:
                            if not cls._recording_jobs:
                                cls._proc_gate_stale_count += 1
                                logger.warning(f"Recovered stale global process gate (reset #{cls._proc_gate_stale_count} & retry)")
                                cls._log_event("proc_gate_stale_recovery", {
                                    "job_id": job_id,
                                    "recovery_count": cls._proc_gate_stale_count
                                })
                                cls._proc_gate = threading.Semaphore(1)
                                proc_gate_acquired = await asyncio.to_thread(
                                    cls._proc_gate.acquire, True, cls._config.SEMAPHORE_ACQUIRE_TIMEOUT
                                )
                    
                    if not proc_gate_acquired:
                        cls._log_event("proc_gate_timeout", {"job_id": job_id})
                        cls._set_state(url, "idle")
                        cls._set_phase(url, RecordingPhase.IDLE)
                        return cls._create_error_result(None, "global_concurrency_timeout", start_time)
            except Exception as e:
                cls._set_state(url, "idle")
                cls._set_phase(url, RecordingPhase.IDLE)
                return cls._create_error_result(None, f"proc_gate_acquire_error:{e}", start_time)

            # ===== ループセマフォ =====
            try:
                await asyncio.wait_for(loop_semaphore.acquire(), timeout=cls._config.SEMAPHORE_ACQUIRE_TIMEOUT)
                sem_acquired = True
            except asyncio.TimeoutError:
                cls._log_event("semaphore_timeout", {"job_id": job_id})
                cls._set_state(url, "idle")
                cls._set_phase(url, RecordingPhase.IDLE)
                return cls._create_error_result(None, "max_concurrent_timeout", start_time)

            # ===== ジョブ登録 =====
            if job_id in cls._recording_jobs:
                cls._set_state(url, "idle")
                cls._set_phase(url, RecordingPhase.IDLE)
                return cls._create_error_result(None, "duplicate_job_id", start_time)

            job = RecordingJob(
                job_id=job_id,
                target=TargetPrefix.URL.value,
                url=url,
                status=RecordingStatus.PREPARING,
                phase=RecordingPhase.STARTING,
                metadata=metadata or {},
                proc_gate_acquired=proc_gate_acquired,
                semaphore_acquired=sem_acquired,
                url_lock_acquired=url_lock_acquired
            )
            cls._recording_jobs[job_id] = job
            cls._log_event("recording_start", {"job_id": job_id, "url": url})
            
            _emit_gui_state(True, url, job_id)
            _gui_started = True
            
            # ===== フェーズ別デッドロック検出開始 =====
            deadlock_timer = asyncio.create_task(
                cls._phase_aware_deadlock_detector(url, job_id)
            )
            job.deadlock_timer = deadlock_timer

            # ===== ログインチェック =====
            if force_login_check:
                job.status = RecordingStatus.LOGIN_CHECK
                await cls._ensure_login(force=True)

            # ===== 録画実行 =====
            recorder = await cls._ensure_recorder()
            
            # Chrome健全性チェック
            if hasattr(recorder, 'chrome'):
                try:
                    ctx = await recorder.chrome.ensure_headless()
                    if not ctx:
                        raise RuntimeError("Chrome context unavailable")
                except Exception as e:
                    logger.error(f"Chrome health check failed: {e}")
                    cls._set_state(url, "error")
                    cls._set_phase(url, RecordingPhase.ERROR)
                    return cls._create_error_result(job, f"chrome_error:{e}", start_time)
            
            job.status = RecordingStatus.RECORDING

            result = {}
            try:
                max_timeout = (duration + 120) if duration else cls._config.ABSOLUTE_RECORDING_TIMEOUT
                
                if hasattr(recorder, "record"):
                    result = await asyncio.wait_for(
                        asyncio.shield(
                            recorder.record(url, duration=duration, meta=job.metadata)
                        ),
                        timeout=max_timeout
                    )
                    
                    # HLS取得成功したら状態遷移
                    if result.get("m3u8") or result.get("hls_url"):
                        cls._set_state(url, "recording")
                        cls._set_phase(url, RecordingPhase.RECORDING)
                        cls._log_event("hls_captured", {"url": url, "job_id": job_id})
                        
                        # 出力ファイルを記録
                        if result.get("output_files"):
                            job.output_files = result["output_files"]
                else:
                    raise RuntimeError("TwitCastingRecorder.record not found")
                    
            except asyncio.TimeoutError:
                cls._log_event("recording_absolute_timeout", {
                    "job_id": job_id,
                    "timeout": max_timeout
                })
                result = {"ok": False, "reason": "absolute_timeout"}
                job.status = RecordingStatus.TIMEOUT
            except asyncio.CancelledError:
                job.status = RecordingStatus.CANCELLED
                cls._log_event("recorder_cancelled", {"job_id": job_id})
                raise
            except Exception as e:
                cls._log_event("recorder_exception", {"job_id": job_id, "error": str(e)})
                result = cls._create_error_result(job, f"recorder_exception:{e}", start_time)

            # ===== 失敗時のJIT救済 =====
            if not (result.get("ok") or result.get("success")):
                reason = (result.get("reason") or result.get("error") or "").lower()
                EARLY = {"network_or_http", "no_bytes", "http_403", "http_401", "403", "401"}
                if any(k in reason for k in EARLY):
                    logger.warning("[HOTFIX] early-fail -> ensure_login(force) & one-shot retry")
                    job.retry_count += 1
                    cls._log_event("jit_retry_start", {
                        "job_id": job_id,
                        "url": url,
                        "reason": reason,
                        "retry_count": job.retry_count,
                    })
                    try:
                        await cls._ensure_login(force=True)
                    except Exception as e:
                        logger.warning("[HOTFIX] ensure_login failed: %s", e)
                        cls._log_event("jit_retry_login_error", {
                            "job_id": job_id, "error": str(e), "reason": reason, "retry_count": job.retry_count
                        })
                    
                    try:
                        await cls._export_latest_cookie_with_validation(recorder)
                    except Exception as e:
                        logger.warning("[HOTFIX] cookie re-export failed: %s", e)
                        cls._log_event("jit_retry_cookie_error", {
                            "job_id": job_id, "error": str(e), "retry_count": job.retry_count
                        })
                    
                    try:
                        result2 = await recorder.record(url, duration=duration, meta=job.metadata)
                        ok2 = bool(result2.get("ok") or result2.get("success"))
                        cls._log_event("jit_retry_done", {
                            "job_id": job_id,
                            "ok": ok2,
                            "reason": (result2.get("reason") or result2.get("error")),
                            "retry_count": job.retry_count
                        })
                        if ok2:
                            result = result2
                            if result.get("output_files"):
                                job.output_files = result["output_files"]
                    except Exception as e:
                        logger.warning("[HOTFIX] retry recorder error: %s", e)
                        cls._log_event("jit_retry_exception", {
                            "job_id": job_id, "error": str(e), "retry_count": job.retry_count
                        })

            # ===== 結果正規化 =====
            job.raw_result = dict(result) if isinstance(result, dict) else {"ok": False}
            ok = bool(result.get("ok") or result.get("success"))
            files = list(result.get("output_files") or result.get("files") or [])
            job.output_files = files[:]
            job.completed_at = time.time()
            job.duration = int(job.completed_at - job.started_at) if job.started_at else None
            job.status = RecordingStatus.COMPLETED if ok else RecordingStatus.ERROR
            job.error = None if ok else (result.get("reason") or result.get("error") or "unknown")

            if ok:
                cls._total_successes += 1
            else:
                cls._total_failures += 1

            norm = {
                **result,
                "ok": ok,
                "success": ok,
                "output_files": files,
                "files": files,
                "job_id": job_id,
                "url": url,
            }
            if not ok:
                norm.setdefault("reason", job.error)

            cls._log_event("recording_result", {"job_id": job_id, "ok": ok, "files": files})
            
            _emit_gui_state(False, url, job_id, ok=ok)
            
            if ok and cls._proc_gate_stale_count > 0:
                cls._proc_gate_stale_count = 0
                logger.info("Stale recovery counter reset after successful recording")
            
            return norm

        except asyncio.CancelledError:
            raise
        except Exception as e:
            cls._log_event("start_record_exception", {"error": str(e), "job_id": job_id})
            return cls._create_error_result(None, f"start_record_exception:{e}", start_time)
        finally:
            # ===== クリーンアップ（タスク破棄エラー抑制・型安全） =====
            # 【修正】deadlock_timerの安全な停止（型チェック追加）
            if deadlock_timer and isinstance(deadlock_timer, asyncio.Task):
                with contextlib.suppress(Exception):
                    deadlock_timer.cancel()
                    # 【修正】awaitして確実に回収
                    try:
                        await asyncio.shield(deadlock_timer)
                    except:
                        pass
            
            # 【修正】file_monitor_taskの安全な停止（型チェック追加）
            if job is not None and hasattr(job, "file_monitor_task"):
                task = getattr(job, "file_monitor_task", None)
                if task and isinstance(task, asyncio.Task):
                    with contextlib.suppress(Exception):
                        task.cancel()
                        # 【修正】awaitして確実に回収
                        try:
                            await asyncio.shield(task)
                        except:
                            pass
            
            # 【修正】その他のタスクのクリーンアップ
            if job is not None and hasattr(job, "deadlock_timer"):
                timer = getattr(job, "deadlock_timer", None)
                if timer and isinstance(timer, asyncio.Task):
                    with contextlib.suppress(Exception):
                        timer.cancel()
            
            try:
                if _gui_started:
                    _emit_gui_state(False, url, job_id)
            except Exception:
                pass
            
            # 状態を確実にidleに戻す
            cls._set_state(url, "idle")
            cls._set_phase(url, RecordingPhase.IDLE)
            
            # 【修正】セマフォの確実な解放（エラー処理強化）
            if sem_acquired:
                try:
                    loop_semaphore.release()
                except RuntimeError as e:
                    if "not acquired" not in str(e):
                        logger.warning(f"loop_semaphore.release() failed: {e}")
                except Exception as e:
                    logger.warning(f"loop_semaphore.release() unexpected error: {e}")
            
            if proc_gate_acquired:
                try:
                    cls._proc_gate.release()
                except RuntimeError as e:
                    if "not acquired" not in str(e):
                        logger.warning(f"proc_gate.release() failed: {e}")
                except Exception as e:
                    logger.warning(f"proc_gate.release() unexpected error: {e}")
            
            if url_lock_acquired:
                try:
                    url_lock.release()
                    with cls._global_state_lock:
                        cls._recording_locks.pop(url, None)
                except RuntimeError as e:
                    if "release unlocked" not in str(e):
                        logger.warning(f"url_lock.release() failed: {e}")
                except Exception as e:
                    logger.warning(f"url_lock.release() unexpected error: {e}")
            
            cls._recording_jobs.pop(job_id, None)

    # ==================== Cookie再出力 ====================
    
    @classmethod
    async def _export_latest_cookie_with_validation(cls, recorder: Any) -> Optional[Path]:
        """_twitcasting_sessionまたはtc_ssを確実に含むCookie出力"""
        try:
            from tc_recorder_core import _save_cookies_netscape
            ctx = await recorder.chrome.ensure_headless()
            
            session_found = False
            tc_ss_found = False
            for i in range(10):
                cookies = await ctx.cookies()
                session_found = any(c["name"] == "_twitcasting_session" for c in cookies)
                tc_ss_found = any(c["name"] == "tc_ss" for c in cookies)
                if session_found or tc_ss_found:
                    logger.info(f"Login cookies confirmed before export (legacy={session_found}, tc_ss={tc_ss_found})")
                    break
                if i < 9:
                    await asyncio.sleep(0.5)
            
            if not session_found and not tc_ss_found:
                logger.warning("Neither _twitcasting_session nor tc_ss found before export")
            
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out = COOKIES_DIR / f"cookies_enter_{ts}.txt"
            await _save_cookies_netscape(ctx, out, ".twitcasting.tv")
            
            with out.open("r", encoding="utf-8") as f:
                content = f.read()
            
            has_legacy = "_twitcasting_session" in content
            has_tc_ss = "tc_ss" in content
            
            if has_legacy or has_tc_ss:
                logger.info(f"✅ Export validated (legacy={has_legacy}, tc_ss={has_tc_ss}): {out}")
            else:
                logger.warning(f"⚠️ Export may be insufficient: {out}")
            
            latest = COOKIES_DIR / "latest_cookie_path.txt"
            latest.write_text(str(out), encoding="utf-8")
            cls._log_event("cookie_exported", {
                "path": str(out),
                "has_legacy": has_legacy,
                "has_tc_ss": has_tc_ss
            })
            cls._log_event("cookie_latest_path_saved", {"path": str(latest)})
            return out
        except Exception as e:
            logger.warning(f"_export_latest_cookie_with_validation failed: {e}")
            return None

    @classmethod
    async def _export_latest_cookie(cls, recorder: Any) -> Optional[Path]:
        """旧メソッド（互換性維持）"""
        return await cls._export_latest_cookie_with_validation(recorder)

    # ==================== URL正規化 ====================
    
    @classmethod
    def _build_url(cls, target: str, hint_url: Optional[str] = None) -> str:
        t = (target or "").strip()
        if not t and hint_url:
            t = hint_url.strip()

        low = t.lower()
        for pre in ("c:", "g:", "ig:", "f:", "tw:"):
            if low.startswith(pre):
                if pre in ("g:", "ig:"):
                    name = t[len(pre):]
                    return f"https://twitcasting.tv/{pre}{name}"
                else:
                    t = t[len(pre):]
                    break

        if t.startswith("http://") or t.startswith("https://"):
            return t
        return f"https://twitcasting.tv/{t}"

    # ==================== エラー理由抽出 ====================
    
    @classmethod
    def _extract_error_reason(cls, result: Any) -> str:
        try:
            if not isinstance(result, dict):
                return "unknown"
            for key in ("reason", "error", "message"):
                if key in result and result[key]:
                    return str(result[key])
        except Exception:
            pass
        return "unknown"

    # ==================== ステータス ====================
    
    @classmethod
    def get_status(cls) -> Dict[str, Any]:
        """現状のジョブ一覧と統計を返す"""
        running = []
        for j in cls._recording_jobs.values():
            running.append({
                "job_id": j.job_id,
                "url": j.url,
                "status": j.status.name,
                "phase": j.phase.value,
                "started_at": j.started_at,
                "completed_at": j.completed_at,
                "files": j.output_files,
                "error": j.error,
                "last_file_size": j.last_file_size,
                "last_file_check": j.last_file_check,
            })
        
        # 状態マップも含める
        states = cls.get_recording_states()
        
        # フェーズマップも含める
        with cls._states_lock:
            phases = {url: phase.value for url, phase in cls._recording_phases.items()}
        
        return {
            "jobs": running,
            "states": states,
            "phases": phases,
            "configured": cls._configured,
            "initialized": cls._is_initialized,
            "concurrency": cls._config.max_concurrent,
            "stale_recovery_count": cls._proc_gate_stale_count,
            "total_recordings": cls._total_recordings,
            "total_successes": cls._total_successes,
            "total_failures": cls._total_failures,
            "last_activity": datetime.fromtimestamp(cls._last_activity_time).isoformat(),
        }

    # ==================== システム健全性情報 ====================
    
    @classmethod
    def get_system_health(cls) -> Dict[str, Any]:
        """システム健全性情報を返す"""
        active_urls = list(cls._recording_locks.keys())
        active_jobs = list(cls._recording_jobs.keys())
        
        try:
            sem_available = cls._proc_gate._value if hasattr(cls._proc_gate, '_value') else None
        except:
            sem_available = None
        
        idle_time = int(time.time() - cls._last_activity_time)
        
        # 状態別カウント
        states = cls.get_recording_states()
        state_counts = {
            "idle": 0,
            "starting": 0,
            "recording": 0,
            "stopping": 0,
            "error": 0,
            "waiting": 0  # 追加
        }
        for state in states.values():
            if state in state_counts:
                state_counts[state] += 1
        
        # フェーズ別カウント
        with cls._states_lock:
            phase_counts = {
                RecordingPhase.IDLE.value: 0,
                RecordingPhase.STARTING.value: 0,
                RecordingPhase.RECORDING.value: 0,
                RecordingPhase.STOPPING.value: 0,
                RecordingPhase.ERROR.value: 0,
                RecordingPhase.WAITING.value: 0,
            }
            for phase in cls._recording_phases.values():
                if phase.value in phase_counts:
                    phase_counts[phase.value] += 1
        
        return {
            "active_jobs": len(active_jobs),
            "active_job_ids": active_jobs,
            "recording_urls": active_urls,
            "state_counts": state_counts,
            "phase_counts": phase_counts,
            "semaphore_available": sem_available,
            "stale_count": cls._proc_gate_stale_count,
            "initialized": cls._is_initialized,
            "configured": cls._configured,
            "idle_seconds": idle_time,
            "health_status": "OK" if idle_time < 300 else "IDLE",
            "timestamp": datetime.now().isoformat()
        }

    # ==================== ゲート解放確認 ====================
    
    @classmethod
    def ensure_all_gates_free(cls) -> bool:
        """全てのセマフォが解放されているか確認（デバッグ用）"""
        try:
            if cls._recording_jobs:
                active_count = len(cls._recording_jobs)
                logger.warning(f"Active jobs remaining: {active_count}")
                return False
            
            if cls._recording_locks:
                locked_urls = list(cls._recording_locks.keys())
                logger.warning(f"URL locks remaining: {locked_urls}")
                return False
            
            if cls._proc_gate_stale_count > 0:
                logger.info(f"Resetting stale counter from {cls._proc_gate_stale_count} to 0")
                cls._proc_gate_stale_count = 0
            
            logger.info("All gates confirmed free")
            return True
        except Exception as e:
            logger.error(f"ensure_all_gates_free error: {e}")
            return False

    # ==================== ログ掃除 ====================
    
    @classmethod
    def cleanup_old_logs(cls) -> int:
        """LOG_KEEP_DAYS 以前の wrapper_*.jsonl を削除"""
        removed = 0
        try:
            threshold = datetime.now() - timedelta(days=max(1, int(cls._config.LOG_KEEP_DAYS)))
            for p in LOGS_DIR.glob("wrapper_*.jsonl"):
                try:
                    ts = p.stem.split("_")[1]
                    dt = datetime.strptime(ts, "%Y%m%d")
                    if dt < threshold.replace(hour=0, minute=0, second=0, microsecond=0):
                        p.unlink(missing_ok=True)
                        removed += 1
                except Exception:
                    continue
        except Exception:
            pass
        if removed:
            cls._log_event("log_cleanup", {"removed": removed})
        return removed

    # ==================== シャットダウン ====================
    
    @classmethod
    async def shutdown(cls) -> None:
        """エンジン停止時にChrome/Playwrightを畳む（セマフォ強制リセット付き）"""
        try:
            cls._shutdown_event.set()
            
            # 全ジョブの停止を待つ
            deadline = time.time() + 8
            while time.time() < deadline:
                active = any(j.status.is_active() for j in cls._recording_jobs.values())
                if not active:
                    break
                await asyncio.sleep(0.2)
            
            # 残っているタスクをキャンセル（エラー抑制・型安全）
            if cls._recording_jobs:
                remaining = len(cls._recording_jobs)
                logger.warning(f"Shutting down with {remaining} active jobs")
                for job in cls._recording_jobs.values():
                    # 【修正】型チェック追加
                    if job.deadlock_timer and isinstance(job.deadlock_timer, asyncio.Task):
                        with contextlib.suppress(Exception):
                            job.deadlock_timer.cancel()
                    if job.file_monitor_task and isinstance(job.file_monitor_task, asyncio.Task):
                        with contextlib.suppress(Exception):
                            job.file_monitor_task.cancel()
            
            # セマフォリセット
            with cls._proc_gate_reset_lock:
                if cls._recording_jobs:
                    logger.warning(f"Force resetting semaphore with {len(cls._recording_jobs)} jobs")
                    cls._recording_jobs.clear()
                cls._proc_gate = threading.Semaphore(cls._config.max_concurrent)
                cls._proc_gate_stale_count = 0
            
            with cls._global_state_lock:
                cls._recording_locks.clear()
            
            # 状態もクリア
            with cls._states_lock:
                cls._recording_states.clear()
                cls._recording_phases.clear()
            
            # Recorder終了
            rec = None
            try:
                rec = await cls._ensure_recorder()
            except Exception:
                rec = None
            if rec and hasattr(rec, "close"):
                try:
                    await rec.close(keep_chrome=False)
                except Exception as e:
                    logger.warning(f"Recorder close error: {e}")
            
            gc.collect()
            
            cls._log_event("shutdown", {
                "total_recordings": cls._total_recordings,
                "total_successes": cls._total_successes,
                "total_failures": cls._total_failures
            })
            logger.info("RecorderWrapper shutdown complete")
            
        except Exception as e:
            logger.warning(f"shutdown warn: {e}", exc_info=True)

    # ==================== 結果ユーティリティ ====================
    
    @classmethod
    def _create_error_result(cls, job: Optional[RecordingJob], reason: str, start_time: float) -> Dict[str, Any]:
        res = {
            "ok": False,
            "success": False,
            "output_files": [],
            "files": [],
            "reason": reason,
            "duration_sec": int(time.time() - start_time) if start_time else None,
        }
        if job:
            job.status = RecordingStatus.ERROR
            job.error = reason
            job.completed_at = time.time()
        return res


# ==================== テスト ====================
if __name__ == "__main__":
    async def test():
        print("=== RecorderWrapper Test v4.8.3 ===")
        
        RecorderWrapper.configure(max_concurrent=1)
        await RecorderWrapper.initialize()
        
        health = RecorderWrapper.get_system_health()
        print(f"System health: {health}")
        
        logged_in = await RecorderWrapper.ensure_login()
        print(f"Login status: {logged_in}")
        
        # Cookie更新テスト
        cookie_ok = await RecorderWrapper.ensure_complete_cookies(force_refresh=False)
        print(f"Cookie update: {cookie_ok}")
        
        if logged_in:
            result = await RecorderWrapper.start_record(
                "https://twitcasting.tv/test",
                duration=10
            )
            print(f"Recording result: {result.get('ok')}")
        
        status = RecorderWrapper.get_status()
        print(f"Status: {status}")
        
        all_free = RecorderWrapper.ensure_all_gates_free()
        print(f"All gates free: {all_free}")
        
        await RecorderWrapper.shutdown()
        print("=== Test Complete ===")
    
    asyncio.run(test())