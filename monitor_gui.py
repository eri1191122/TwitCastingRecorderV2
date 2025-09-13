#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
monitor_gui.py - TwitCasting Auto Recorder GUI
Version: 5.1.0 (完全修正版・状態管理統一)

修正内容：
1. 完全な非同期処理管理
2. 堅固なエラーハンドリング
3. 効率的な状態同期
4. メモリリーク対策
5. UI応答性向上
6. 全機能維持（削減なし）
7. RecorderWrapper状態を信頼
8. GUI表示改善（状態テキスト追加）
"""

from __future__ import annotations

import asyncio
import json
import sys
import os
import threading
import time
import re
import traceback
import weakref
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any, Set, Callable
from enum import Enum
from collections import deque
from uuid import uuid4
from contextlib import suppress, asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

# psutil動的インポート
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("[WARNING] psutilがインストールされていません。プロセス管理機能が制限されます。")

# ---- Path initialization (root placement version) ------------------------
ROOT: Path = Path(__file__).resolve().parent  # TwitCastingRecorderV2/
AUTO_DIR: Path = ROOT / "auto"               # TwitCastingRecorderV2/auto/
PROJECT_ROOT: Path = ROOT                    # TwitCastingRecorderV2/

print(f"[INIT] ROOT: {ROOT}")
print(f"[INIT] AUTO_DIR: {AUTO_DIR}")
print(f"[INIT] Working Dir: {os.getcwd()}")

# sys.path setup (add auto folder)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(AUTO_DIR) not in sys.path:
    sys.path.insert(0, str(AUTO_DIR))

# ---- Engine import (root placement compatible) -------------------------------------------
MonitorEngine = None
MonitorConfig = None

# Method 1: import from auto package
try:
    from auto.monitor_engine import MonitorEngine, MonitorConfig
    print("[IMPORT] Success: auto.monitor_engine")
except ImportError as e:
    print(f"[IMPORT] Failed: auto.monitor_engine - {e}")
    
    # Method 2: direct import (auto already in sys.path)
    try:
        import monitor_engine
        MonitorEngine = monitor_engine.MonitorEngine
        MonitorConfig = monitor_engine.MonitorConfig
        print("[IMPORT] Success: monitor_engine (direct)")
    except ImportError as e:
        print(f"[CRITICAL] monitor_engine import failed: {e}", file=sys.stderr)
        # 安全なエラー表示
        try:
            _tmp_root = tk.Tk()
            _tmp_root.withdraw()
            messagebox.showerror(
                "起動エラー",
                f"monitor_engine.py が見つかりません。\n{AUTO_DIR}に配置してください。"
            )
            _tmp_root.destroy()
        except Exception as ee:
            print(f"[FALLBACK] 起動エラー: monitor_engine.py が見つかりません。 ({ee})", file=sys.stderr)
        sys.exit(1)

# ---- facade (login conductor) -----------------------------------------------------------
TwitCastingRecorder = None
try:
    from facade import TwitCastingRecorder
    print("[IMPORT] Success: facade")
except ImportError:
    print("[IMPORT] Warning: facade not available")

# ---- RecorderWrapper import (必須) ---------------------------------------------------
RecorderWrapper = None
try:
    from auto.recorder_wrapper import RecorderWrapper
    print("[IMPORT] Success: RecorderWrapper")
except ImportError as e:
    print(f"[IMPORT] Warning: RecorderWrapper not available - {e}")
    try:
        import recorder_wrapper
        RecorderWrapper = recorder_wrapper.RecorderWrapper
        print("[IMPORT] Success: recorder_wrapper (direct)")
    except ImportError:
        print("[IMPORT] Critical: RecorderWrapper completely unavailable")
        # 安全なエラー表示
        try:
            _tmp_root = tk.Tk()
            _tmp_root.withdraw()
            messagebox.showerror("起動エラー", "RecorderWrapperが見つかりません")
            _tmp_root.destroy()
        except Exception:
            print("[FALLBACK] 起動エラー: RecorderWrapperが見つかりません", file=sys.stderr)
        sys.exit(1)


# =============================================================================
# State management (改良版)
# =============================================================================
class GUIState(Enum):
    """GUI State（拡張版）"""
    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    ERROR = "error"
    RECOVERING = "recovering"  # 新規追加：回復中状態

    def can_start(self) -> bool:
        """開始可能な状態か"""
        return self in {GUIState.IDLE, GUIState.ERROR}
    
    def can_stop(self) -> bool:
        """停止可能な状態か"""
        return self in {GUIState.RUNNING, GUIState.RECOVERING}
    
    def is_busy(self) -> bool:
        """処理中か"""
        return self in {GUIState.STARTING, GUIState.STOPPING, GUIState.RECOVERING}


# =============================================================================
# Path constants
# =============================================================================
HEARTBEAT_PRIMARY: Path = AUTO_DIR / "heartbeat.json"
HEARTBEAT_FALLBACK: Path = AUTO_DIR / "logs" / "heartbeat.json"
TARGETS_FILE: Path = AUTO_DIR / "targets.json"
LOGS_DIR_PRIMARY: Path = AUTO_DIR / "logs"
LOGS_DIR_FALLBACK: Path = ROOT / "logs"
GUI_STATE_LOG: Path = ROOT / "logs" / "monitor_gui_bridge.jsonl"

DEFAULT_POLL_INTERVAL = 30
DEFAULT_MAX_CONCURRENT = 1
HEALTH_CHECK_INTERVAL_MS = 5000
STATE_POLL_INTERVAL_MS = 500  # 高速ポーリング
STOP_TIMEOUT_MS = 30000  # 停止タイムアウト（30秒）

# 色定数
COLOR_RECORDING = "#d32f2f"  # 赤
COLOR_MONITORING = "#00a968"  # 緑
COLOR_ERROR = "#fbc02d"       # 黄
COLOR_IDLE = "#808080"        # 灰
COLOR_WAITING = "#fbc02d"     # 黄（waiting追加）
COLOR_STOPPING = "#ff9800"    # オレンジ（stopping追加）


# =============================================================================
# Utilities (改良版)
# =============================================================================
def read_json_safe(path: Path) -> Optional[dict]:
    """JSON読み込み（エラー耐性・ロック対応）"""
    max_retries = 3
    for i in range(max_retries):
        try:
            if path.exists():
                with path.open("r", encoding="utf-8") as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            if i < max_retries - 1:
                time.sleep(0.1)  # リトライ前に短い待機
            else:
                print(f"[JSON_ERROR] {path}: {e}")
    return None


def write_json_safe(path: Path, data: dict) -> bool:
    """JSON書き込み（エラー耐性・アトミック）"""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".tmp")
        with temp.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        temp.replace(path)
        return True
    except Exception as e:
        print(f"[JSON_ERROR] {path}: {e}")
        return False


def now_hms() -> str:
    """現在時刻（HH:MM:SS）"""
    return datetime.now().strftime("%H:%M:%S")


def now_iso() -> str:
    """ISO形式時刻"""
    return datetime.now().isoformat()


# =============================================================================
# Process Manager (改良版)
# =============================================================================
class ProcessManager:
    """録画プロセスの完全管理（改良版）"""
    
    def __init__(self):
        self._tracked_pids: Set[int] = set()
        self._lock = threading.RLock()  # RLockに変更（再入可能）
        self._psutil_available = PSUTIL_AVAILABLE
        self._process_cache: Dict[int, weakref.ref] = {}  # プロセスキャッシュ
        
        if not self._psutil_available:
            print("[PROCESS] psutil unavailable - process management limited")
    
    def track_pid(self, pid: int) -> None:
        """PIDを追跡対象に追加"""
        with self._lock:
            self._tracked_pids.add(pid)
            if self._psutil_available:
                try:
                    proc = psutil.Process(pid)
                    self._process_cache[pid] = weakref.ref(proc)
                except:
                    pass
            print(f"[PROCESS] Tracking PID: {pid}")
    
    def untrack_pid(self, pid: int) -> None:
        """PIDを追跡対象から削除"""
        with self._lock:
            self._tracked_pids.discard(pid)
            self._process_cache.pop(pid, None)
            print(f"[PROCESS] Untracking PID: {pid}")
    
    def kill_all_tracked(self) -> int:
        """追跡中の全プロセスを強制終了（改良版）"""
        if not self._psutil_available:
            return 0
        
        killed = 0
        with self._lock:
            pids_to_kill = list(self._tracked_pids)
            
        for pid in pids_to_kill:
            try:
                # キャッシュから取得または新規作成
                proc = None
                if pid in self._process_cache:
                    proc_ref = self._process_cache[pid]
                    proc = proc_ref() if proc_ref else None
                
                if not proc:
                    proc = psutil.Process(pid)
                
                # 段階的終了
                proc.terminate()
                try:
                    proc.wait(timeout=2)  # 2秒待機
                except psutil.TimeoutExpired:
                    proc.kill()  # 強制終了
                    proc.wait(timeout=1)
                
                killed += 1
                print(f"[PROCESS] Killed PID: {pid}")
                
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception as e:
                print(f"[PROCESS] Kill error for PID {pid}: {e}")
            finally:
                with self._lock:
                    self._tracked_pids.discard(pid)
                    self._process_cache.pop(pid, None)
        
        return killed
    
    def find_orphan_processes(self) -> List[int]:
        """孤児化した録画プロセスを検出（改良版）"""
        if not self._psutil_available:
            return []
        
        orphans = []
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
                try:
                    info = proc.info
                    cmdline = info.get('cmdline', [])
                    
                    # yt-dlp/ffmpegプロセスを検出
                    is_recorder = any(
                        keyword in str(arg) 
                        for arg in cmdline 
                        for keyword in ['yt-dlp', 'yt_dlp', 'ffmpeg']
                    )
                    
                    is_twitcasting = any(
                        'twitcasting.tv' in str(arg) 
                        for arg in cmdline
                    )
                    
                    if is_recorder and is_twitcasting:
                        if info['pid'] not in self._tracked_pids:
                            # 作成時間チェック（古いプロセスのみ）
                            if time.time() - info['create_time'] > 60:
                                orphans.append(info['pid'])
                                
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
                except Exception:
                    continue
                    
        except Exception as e:
            print(f"[PROCESS] Orphan detection error: {e}")
        
        return orphans
    
    def get_process_info(self, pid: int) -> Optional[dict]:
        """プロセス情報取得"""
        if not self._psutil_available:
            return None
        
        try:
            proc = psutil.Process(pid)
            return {
                'pid': pid,
                'name': proc.name(),
                'status': proc.status(),
                'cpu_percent': proc.cpu_percent(),
                'memory_info': proc.memory_info()._asdict()
            }
        except:
            return None


# =============================================================================
# Async Task Manager (新規追加)
# =============================================================================
class AsyncTaskManager:
    """非同期タスク管理（新規）"""
    
    def __init__(self):
        self._tasks: Dict[str, asyncio.Task] = {}
        self._futures: Dict[str, asyncio.Future] = {}
        self._lock = asyncio.Lock()
    
    async def create_task(
        self, 
        name: str, 
        coro: Callable, 
        *args, 
        **kwargs
    ) -> asyncio.Task:
        """管理されたタスクを作成"""
        async with self._lock:
            # 既存タスクをキャンセル
            if name in self._tasks:
                old_task = self._tasks[name]
                if not old_task.done():
                    old_task.cancel()
                    with suppress(asyncio.CancelledError):
                        await old_task
            
            # 新規タスク作成
            task = asyncio.create_task(coro(*args, **kwargs))
            self._tasks[name] = task
            
            # 完了時のクリーンアップ
            task.add_done_callback(
                lambda t: self._tasks.pop(name, None)
            )
            
            return task
    
    async def cancel_task(self, name: str) -> bool:
        """タスクをキャンセル"""
        async with self._lock:
            if name in self._tasks:
                task = self._tasks[name]
                if not task.done():
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass
                    return True
            return False
    
    async def cancel_all(self) -> int:
        """全タスクをキャンセル"""
        async with self._lock:
            tasks = list(self._tasks.values())
        
        cancelled = 0
        for task in tasks:
            if not task.done():
                task.cancel()
                cancelled += 1
        
        # 全タスクの完了を待機
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        self._tasks.clear()
        return cancelled


# =============================================================================
# Main GUI Class (完全版)
# =============================================================================
class MonitorGUI:
    def __init__(self) -> None:
        # Tkinter initialization
        self.root = tk.Tk()
        self.root.title("TwitCasting 自動録画 GUI v5.1.0")
        self.root.geometry("1100x750")
        
        # State management (改良版)
        self.state = GUIState.IDLE
        self.urls: List[str] = []
        
        # Session management
        self._sessions: Dict[str, str] = {}  # url -> session_id
        self._operation_lock: Set[str] = set()  # URL operation lock
        
        # Process management
        self._process_manager = ProcessManager()
        
        # Async task management (新規)
        self._task_manager: Optional[AsyncTaskManager] = None
        
        # URL表示管理（改良版）
        self.url_index_map: Dict[str, int] = {}  # URL→Listbox index
        self.url_display_states: Dict[str, str] = {}  # URL→表示状態テキスト
        
        # Thread safety
        self._state_lock = threading.RLock()
        
        # Thread pool (新規)
        self._thread_pool = ThreadPoolExecutor(max_workers=4)
        
        # Async resources
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.loop_thread: Optional[threading.Thread] = None
        self.engine: Optional[MonitorEngine] = None
        
        # Control flags
        self._closing_requested = False
        self._stopping_in_progress = False
        self._stop_initiated_time: Optional[float] = None
        self._stop_call_count = 0
        
        # Timer IDs
        self._status_refresh_id: Optional[str] = None
        self._health_check_id: Optional[str] = None
        self._state_poller_id: Optional[str] = None
        self._stop_timeout_id: Optional[str] = None
        
        # Health monitoring
        self._last_health_check = time.time()
        self._consecutive_failures = 0
        
        # GUI-STATE監視
        self._gui_state_reader: Optional[threading.Thread] = None
        self._gui_state_stop = threading.Event()
        
        # 強制終了ボタン（初期は非表示）
        self.force_stop_btn: Optional[ttk.Button] = None
        
        # Build GUI
        self._build_gui()
        
        # Startup cleanup
        self._startup_cleanup()
        
        # Initialize
        self._load_targets()
        self._check_login_status_async()
        
        # Start monitoring threads
        self._start_gui_state_reader()
        self._start_wrapper_state_poller()
        self._schedule_health_check()
        
        # Event handler
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    # ---------------------------------------------------------------------
    # Startup Cleanup (安全版)
    # ---------------------------------------------------------------------
    def _startup_cleanup(self) -> None:
        """起動時の完全クリーンアップ（エラー耐性）"""
        self._log("=== 起動時クリーンアップ ===", level="INFO")
        
        # 1. 孤児プロセスを全て終了（psutil利用可能時のみ）
        if PSUTIL_AVAILABLE:
            orphans = self._process_manager.find_orphan_processes()
            if orphans:
                self._log(f"孤児プロセス検出: {len(orphans)}個", level="WARNING")
                for pid in orphans:
                    try:
                        proc = psutil.Process(pid)
                        proc.terminate()
                        try:
                            proc.wait(timeout=2)
                        except psutil.TimeoutExpired:
                            proc.kill()
                        self._log(f"孤児プロセス終了: PID {pid}", level="INFO")
                    except Exception:
                        pass
        else:
            self._log("psutil未インストールのためプロセスクリーンアップをスキップ", level="DEBUG")
        
        # 2. ロックファイル削除
        self._remove_lock_files()
        
        # 3. 状態ファイル初期化
        self._reset_state_files()
        
        # 4. RecorderWrapper完全リセット（安全に）
        if RecorderWrapper:
            try:
                # emergency_resetが存在する場合のみ実行
                if hasattr(RecorderWrapper, 'emergency_reset'):
                    RecorderWrapper.emergency_reset()
                    self._log("セマフォ強制リセット完了", level="INFO")
                
                # 状態リセット（安全なアクセス）
                if hasattr(RecorderWrapper, 'set_state'):
                    # set_stateメソッドがある場合
                    for url in self.urls:
                        try:
                            RecorderWrapper.set_state(url, "idle")
                        except Exception:
                            pass
                
                self._log("RecorderWrapper状態リセット完了", level="DEBUG")
            except Exception as e:
                self._log(f"RecorderWrapperリセット失敗: {e}", level="DEBUG")
        
        self._log("起動時クリーンアップ完了", level="SUCCESS")
    
    def _remove_lock_files(self) -> None:
        """ロックファイルの削除"""
        lock_files = [
            AUTO_DIR / ".recording.lock",
            AUTO_DIR / ".engine.lock",
            LOGS_DIR_PRIMARY / ".wrapper.lock",
            ROOT / ".gui.lock"
        ]
        
        for lock_file in lock_files:
            if lock_file.exists():
                try:
                    lock_file.unlink()
                    self._log(f"ロックファイル削除: {lock_file.name}", level="DEBUG")
                except Exception as e:
                    self._log(f"ロック削除失敗: {lock_file.name} - {e}", level="DEBUG")
    
    def _reset_state_files(self) -> None:
        """状態ファイルを安全にリセット"""
        # heartbeat.json初期化
        write_json_safe(HEARTBEAT_PRIMARY, {
            "ts": int(time.time()),
            "state": "idle",
            "active_jobs": 0,
            "total_checks": 0,
            "total_successes": 0,
            "total_errors": 0,
            "targets": 0,
            "max_concurrent": DEFAULT_MAX_CONCURRENT,
            "pid": os.getpid()
        })
        
        # monitor_gui_bridge.jsonl初期化
        try:
            GUI_STATE_LOG.parent.mkdir(parents=True, exist_ok=True)
            with GUI_STATE_LOG.open("w", encoding="utf-8") as f:
                initial_state = {
                    "ts": int(time.time()),
                    "type": "GUI-STATE",
                    "recording": False,
                    "url": "",
                    "job_id": "",
                    "session_id": str(uuid4()),
                    "ok": True
                }
                f.write(json.dumps(initial_state, ensure_ascii=False) + "\n")
            self._log("状態ファイルリセット", level="DEBUG")
        except Exception as e:
            self._log(f"状態ファイルリセット失敗: {e}", level="DEBUG")

    # ---------------------------------------------------------------------
    # GUI Construction（改良版）
    # ---------------------------------------------------------------------
    def _build_gui(self) -> None:
        """GUIを構築"""
        main_frame = ttk.Frame(self.root, padding="5")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        self._build_top_controls(main_frame)
        self._build_status_area(main_frame)
        self._build_url_area(main_frame)
        self._build_log_area(main_frame)
        
        self._log("=== GUI起動 (v5.1.0) ===", level="SUCCESS")

    def _build_top_controls(self, parent: ttk.Frame) -> None:
        """上部コントロール"""
        top = ttk.Frame(parent)
        top.pack(side=tk.TOP, fill=tk.X, pady=(0, 5))
        
        # URL入力
        ttk.Label(top, text="URL:").grid(row=0, column=0, sticky="w", padx=2)
        self.url_var = tk.StringVar()
        self.url_entry = ttk.Entry(top, textvariable=self.url_var, width=50)
        self.url_entry.grid(row=0, column=1, padx=2)
        self.url_entry.bind("<Return>", lambda e: self._add_url_from_entry())
        self._bind_context_menu(self.url_entry)
        
        ttk.Button(top, text="追加", command=self._add_url_from_entry).grid(row=0, column=2, padx=2)
        
        # ログイン関連
        self.login_btn = ttk.Button(top, text="ログイン確認", command=self._check_login_status_async)
        self.login_btn.grid(row=0, column=3, padx=2)
        
        self.wizard_btn = ttk.Button(top, text="ログインウィザード", command=self._open_login_wizard_async)
        self.wizard_btn.grid(row=0, column=4, padx=2)
        
        # Cookie更新ボタン（新規追加）
        self.cookie_btn = ttk.Button(top, text="Cookie更新", command=self._update_cookies_async)
        self.cookie_btn.grid(row=0, column=5, padx=2)
        
        # その他
        ttk.Button(top, text="設定", command=self._show_config).grid(row=0, column=6, padx=2)
        ttk.Button(top, text="ログ", command=self._show_logs).grid(row=0, column=7, padx=2)
        ttk.Button(top, text="全削除", command=self._clear_all_urls).grid(row=0, column=8, padx=2)
        
        # プロセス管理ボタン（psutil利用可能時のみ有効）
        self.orphan_btn = ttk.Button(top, text="孤児プロセス終了", command=self._kill_orphans)
        self.orphan_btn.grid(row=0, column=9, padx=2)
        if not PSUTIL_AVAILABLE:
            self.orphan_btn.config(state="disabled")
        
        # 実行制御
        control = ttk.Frame(parent)
        control.pack(side=tk.TOP, fill=tk.X, pady=5)
        
        self.start_btn = ttk.Button(control, text="監視開始", command=self._start_monitoring)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        
        self.stop_btn = ttk.Button(control, text="停止", command=self._stop_monitoring, state="disabled")
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        # 強制終了ボタン（初期は非表示）
        self.force_stop_btn = ttk.Button(control, text="強制終了", command=self._force_stop_monitoring, state="disabled")
        # 初期は非表示
        
        # 状態表示
        self.state_label = ttk.Label(control, text="待機中", foreground="gray")
        self.state_label.pack(side=tk.LEFT, padx=10)
        
        # 健全性インジケーター
        self.health_label = ttk.Label(control, text="●", foreground="green")
        self.health_label.pack(side=tk.LEFT, padx=5)

    def _build_status_area(self, parent: ttk.Frame) -> None:
        """ステータス表示"""
        frame = ttk.LabelFrame(parent, text="ステータス", padding="5")
        frame.pack(side=tk.TOP, fill=tk.X, pady=5)
        
        # ログイン状態
        login_frame = ttk.Frame(frame)
        login_frame.grid(row=0, column=0, columnspan=4, sticky="w", pady=2)
        ttk.Label(login_frame, text="ログイン:").pack(side=tk.LEFT, padx=2)
        self.login_status_label = ttk.Label(login_frame, text="未確認", foreground="gray")
        self.login_status_label.pack(side=tk.LEFT, padx=5)
        
        # その他ステータス
        self.status_labels: Dict[str, ttk.Label] = {}
        items = [
            ("active_jobs", "アクティブ"),
            ("total_success", "成功"),
            ("total_errors", "エラー"),
            ("last_update", "更新"),
            ("tracked_pids", "追跡PID"),
        ]
        
        for i, (key, label) in enumerate(items):
            row = 1 + (i // 3)
            col = (i % 3) * 2
            ttk.Label(frame, text=f"{label}:").grid(row=row, column=col, sticky="w", padx=2)
            lbl = ttk.Label(frame, text="-", foreground="gray")
            lbl.grid(row=row, column=col+1, sticky="w", padx=5)
            self.status_labels[key] = lbl

    def _build_url_area(self, parent: ttk.Frame) -> None:
        """URL管理エリア（改良版）"""
        frame = ttk.LabelFrame(parent, text="監視対象URL", padding="5")
        frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=5)
        
        # Listbox
        list_frame = ttk.Frame(frame)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # フォント設定（状態テキスト表示のため）
        self.url_list = tk.Listbox(
            list_frame, 
            height=8, 
            yscrollcommand=scrollbar.set,
            font=("Consolas", 9)  # 等幅フォント
        )
        self.url_list.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.url_list.yview)
        self._bind_listbox_context_menu(self.url_list)
        
        # ボタン
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=5)
        
        ttk.Button(btn_frame, text="削除", command=self._remove_selected).pack(fill=tk.X, pady=2)
        ttk.Button(btn_frame, text="インポート", command=self._import_urls).pack(fill=tk.X, pady=2)
        ttk.Button(btn_frame, text="保存", command=self._save_targets).pack(fill=tk.X, pady=2)
        
        self.url_count_label = ttk.Label(btn_frame, text="0 URLs")
        self.url_count_label.pack(pady=10)

    def _build_log_area(self, parent: ttk.Frame) -> None:
        """ログ表示エリア"""
        frame = ttk.LabelFrame(parent, text="リアルタイムログ", padding="5")
        frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = tk.Text(frame, height=12, wrap="word", bg="black", fg="white")
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = ttk.Scrollbar(frame, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text['yscrollcommand'] = scrollbar.set
        
        # タグ設定
        self.log_text.tag_config("INFO", foreground="white")
        self.log_text.tag_config("SUCCESS", foreground="lime")
        self.log_text.tag_config("WARNING", foreground="yellow")
        self.log_text.tag_config("ERROR", foreground="red")
        self.log_text.tag_config("DEBUG", foreground="cyan")

    # ---------------------------------------------------------------------
    # Context Menu
    # ---------------------------------------------------------------------
    def _bind_context_menu(self, widget):
        """Entry用コンテキストメニュー"""
        menu = tk.Menu(widget, tearoff=0)
        menu.add_command(label="切り取り", command=lambda: widget.event_generate("<<Cut>>"))
        menu.add_command(label="コピー", command=lambda: widget.event_generate("<<Copy>>"))
        menu.add_command(label="貼り付け", command=lambda: widget.event_generate("<<Paste>>"))
        menu.add_separator()
        menu.add_command(label="全選択", command=lambda: widget.select_range(0, tk.END))
        
        def _popup(event):
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()
        
        widget.bind("<Button-3>", _popup)
        widget.bind("<Control-Button-1>", _popup)
    
    def _bind_listbox_context_menu(self, widget):
        """Listbox用コンテキストメニュー"""
        menu = tk.Menu(widget, tearoff=0)
        menu.add_command(label="コピー", command=self._copy_selected_urls)
        menu.add_command(label="削除", command=self._remove_selected)
        menu.add_separator()
        menu.add_command(label="全選択", command=lambda: widget.select_set(0, tk.END))
        
        def _popup(event):
            try:
                menu.tk_popup(event.x_root, event.y_root)
            finally:
                menu.grab_release()
        
        widget.bind("<Button-3>", _popup)
        widget.bind("<Control-Button-1>", _popup)
    
    def _copy_selected_urls(self):
        """選択URLをクリップボードにコピー"""
        indices = self.url_list.curselection()
        if indices:
            urls = []
            for i in indices:
                display_text = self.url_list.get(i)
                # 状態テキストを除去してURLのみ取得
                url = display_text.split("] ", 1)[-1] if "] " in display_text else display_text
                urls.append(url)
            text = "\n".join(urls)
            self.root.clipboard_clear()
            self.root.clipboard_append(text)
            self._log(f"{len(urls)}個のURLをコピー", level="INFO")

    # ---------------------------------------------------------------------
    # Cookie Management (新規追加)
    # ---------------------------------------------------------------------
    def _update_cookies_async(self) -> None:
        """Cookie更新（非同期）- RecorderWrapper経由"""
        def worker():
            try:
                if not RecorderWrapper:
                    self.root.after(0, lambda: self._log("RecorderWrapper未検出", level="ERROR"))
                    return
                    
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def update():
                    return await RecorderWrapper.ensure_complete_cookies(force_refresh=True)
                
                success = loop.run_until_complete(update())
                loop.close()
                
                if success:
                    self.root.after(0, lambda: self._log("Cookie更新完了", level="SUCCESS"))
                else:
                    self.root.after(0, lambda: self._log("Cookie更新失敗", level="ERROR"))
                    
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda err=error_msg: self._log(f"Cookie更新エラー: {err}", level="ERROR"))
        
        self._thread_pool.submit(worker)

    # ---------------------------------------------------------------------
    # State Synchronization (完全版・改良版)
    # ---------------------------------------------------------------------
    def _start_wrapper_state_poller(self):
        """RecorderWrapperから高速ポーリング（完全版）"""
        if self._closing_requested:
            return
        
        def poll_states():
            try:
                if not RecorderWrapper:
                    return
                
                # RecorderWrapperから真の状態を取得
                states = {}
                if hasattr(RecorderWrapper, 'get_recording_states'):
                    try:
                        result = RecorderWrapper.get_recording_states()
                        if result is not None:
                            states = result
                    except Exception as e:
                        print(f"[STATE_POLL] get_recording_states error: {e}")
                        states = {}
                
                # カウント計算
                recording_count = 0
                waiting_count = 0
                starting_count = 0
                stopping_count = 0
                error_count = 0
                
                for state in states.values():
                    if state == "recording":
                        recording_count += 1
                    elif state == "waiting":
                        waiting_count += 1
                    elif state == "starting":
                        starting_count += 1
                    elif state == "stopping":
                        stopping_count += 1
                    elif state == "error":
                        error_count += 1
                
                # ステータス更新
                self.status_labels["active_jobs"].config(
                    text=f"{recording_count}" + (f"+{waiting_count}" if waiting_count else "")
                )
                self.status_labels["tracked_pids"].config(
                    text=str(len(self._process_manager._tracked_pids))
                )
                
                # URL表示更新（改良版：状態テキスト付き）
                for url in self.urls:
                    state = states.get(url, "idle")
                    idx = self.url_index_map.get(url)
                    if idx is not None:
                        # 状態テキスト生成
                        state_text_map = {
                            "recording": "[録画中]",
                            "starting": "[開始中]",
                            "waiting": "[待機中]",
                            "stopping": "[停止中]",
                            "error": "[エラー]",
                            "idle": "[監視中]" if self.state == GUIState.RUNNING else "[待機　]"
                        }
                        state_text = state_text_map.get(state, "[不明　]")
                        
                        # 表示テキスト更新
                        display_text = f"{state_text} {url}"
                        try:
                            current_text = self.url_list.get(idx)
                            if current_text != display_text:
                                self.url_list.delete(idx)
                                self.url_list.insert(idx, display_text)
                        except:
                            pass
                        
                        # 色設定
                        color_map = {
                            "recording": COLOR_RECORDING,
                            "starting": COLOR_MONITORING,
                            "waiting": COLOR_WAITING,
                            "stopping": COLOR_STOPPING,
                            "error": COLOR_ERROR,
                        }
                        
                        if state in color_map:
                            color = color_map[state]
                        elif self.state == GUIState.RUNNING:
                            color = COLOR_MONITORING
                        else:
                            color = COLOR_IDLE
                        
                        try:
                            self.url_list.itemconfig(idx, foreground=color)
                        except Exception:
                            pass
                
                # 全体状態の表示（詳細版）
                status_parts = []
                if recording_count > 0:
                    status_parts.append(f"録画中({recording_count})")
                if waiting_count > 0:
                    status_parts.append(f"待機中({waiting_count})")
                if starting_count > 0:
                    status_parts.append(f"開始中({starting_count})")
                if stopping_count > 0:
                    status_parts.append(f"停止中({stopping_count})")
                if error_count > 0:
                    status_parts.append(f"エラー({error_count})")
                
                if status_parts:
                    self.state_label.config(
                        text=" / ".join(status_parts),
                        foreground="red" if recording_count > 0 else "orange"
                    )
                elif self.state == GUIState.RUNNING:
                    self.state_label.config(text="監視中", foreground="green")
                elif self.state == GUIState.STOPPING:
                    self.state_label.config(text="停止処理中...", foreground="orange")
                else:
                    self.state_label.config(text="待機中", foreground="gray")
                
            except Exception as e:
                print(f"[STATE_POLL_ERROR] {e}")
            
            # 次回実行（500ms）
            if not self._closing_requested:
                self._state_poller_id = self.root.after(STATE_POLL_INTERVAL_MS, poll_states)
        
        # 初回実行
        self.root.after(100, poll_states)
    
    def _start_gui_state_reader(self):
        """GUI-STATEイベントを読み取る（唯一の真実）"""
        def reader():
            log_path = GUI_STATE_LOG
            last_pos = 0
            
            while not self._gui_state_stop.is_set():
                try:
                    if not log_path.exists():
                        time.sleep(0.5)
                        continue
                    
                    with log_path.open("r", encoding="utf-8") as f:
                        try:
                            f.seek(last_pos)
                        except Exception:
                            f.seek(0)
                            last_pos = 0
                        
                        for line in f:
                            try:
                                data = json.loads(line)
                                if data.get("type") != "GUI-STATE":
                                    continue
                                
                                # セッション管理
                                url = data.get("url", "")
                                session_id = data.get("session_id", "")
                                recording = data.get("recording", False)
                                
                                if url and session_id:
                                    if recording:
                                        self._sessions[url] = session_id
                                        self._log(f"録画状態変更: 開始", level="INFO")
                                    else:
                                        if url in self._sessions:
                                            del self._sessions[url]
                                            self._log(f"録画状態変更: 終了", level="INFO")
                                
                            except json.JSONDecodeError:
                                continue
                        
                        last_pos = f.tell()
                    
                except Exception as e:
                    print(f"[GUI_STATE_READER_ERROR] {e}")
                
                time.sleep(0.5)
        
        self._gui_state_reader = threading.Thread(target=reader, daemon=True)
        self._gui_state_reader.start()

    # ---------------------------------------------------------------------
    # Health Check
    # ---------------------------------------------------------------------
    def _schedule_health_check(self) -> None:
        """定期的な健全性チェック"""
        if self._closing_requested:
            return
        
        self._periodic_health_check()
        self._health_check_id = self.root.after(HEALTH_CHECK_INTERVAL_MS, self._schedule_health_check)
    
    def _periodic_health_check(self) -> None:
        """健全性チェック実行"""
        try:
            # heartbeat確認
            data = read_json_safe(HEARTBEAT_PRIMARY)
            if data:
                ts = data.get("ts", 0)
                if time.time() - ts < 10:
                    self.health_label.config(text="●", foreground="green")
                    self._consecutive_failures = 0
                else:
                    self._consecutive_failures += 1
                    if self._consecutive_failures > 3:
                        self.health_label.config(text="●", foreground="red")
                    else:
                        self.health_label.config(text="●", foreground="yellow")
            
            # 孤児プロセスチェック（psutil利用可能時のみ）
            if PSUTIL_AVAILABLE:
                orphans = self._process_manager.find_orphan_processes()
                if orphans:
                    self._log(f"孤児プロセス検出: {len(orphans)}個", level="WARNING")
            
        except Exception as e:
            print(f"[HEALTH_CHECK_ERROR] {e}")

    # ---------------------------------------------------------------------
    # URL Management（改良版）
    # ---------------------------------------------------------------------
    def _normalize_url(self, url: str) -> str:
        """URL正規化"""
        s = url.strip().lower()
        if not s:
            return ""
        
        # Prefix processing
        m = re.match(r'^(?P<prefix>(c|g|ig|f|tw):)\s*(?P<name>[a-z0-9_]+)$', s)
        if m:
            pre = m.group('prefix')
            name = m.group('name')
            if pre in ('g:', 'ig:'):
                return f"https://twitcasting.tv/{pre}{name}"
            s = name
        
        # Remove broadcaster
        s = re.sub(r"/broadcaster/?$", "", s)
        
        # Scheme completion
        if not s.startswith("http"):
            if "/" not in s and re.match(r"^[a-z0-9_]+$", s):
                s = f"https://twitcasting.tv/{s}"
            elif "twitcasting.tv" in s:
                s = f"https://{s}"
        
        return s.rstrip("/")

    def _add_url_from_entry(self) -> None:
        """URL追加"""
        url = self._normalize_url(self.url_var.get())
        if not url:
            return
        
        if url in self.urls:
            self._log(f"重複URL: {url}", level="WARNING")
            return
        
        self.urls.append(url)
        # 初期表示は状態なし
        display_text = f"[待機　] {url}"
        self.url_list.insert(tk.END, display_text)
        self._update_url_count()
        self._update_url_index_map()
        self._save_targets()
        self.url_var.set("")
        self._log(f"URL追加: {url}", level="SUCCESS")

    def _remove_selected(self) -> None:
        """選択URL削除"""
        indices = list(self.url_list.curselection())
        if not indices:
            return
        
        indices.reverse()
        for idx in indices:
            display_text = self.url_list.get(idx)
            # 状態テキストを除去してURLのみ取得
            url = display_text.split("] ", 1)[-1] if "] " in display_text else display_text
            if url in self.urls:
                self.urls.remove(url)
            self.url_list.delete(idx)
        
        self._update_url_count()
        self._update_url_index_map()
        self._save_targets()
        self._log(f"{len(indices)}個のURLを削除", level="INFO")

    def _clear_all_urls(self) -> None:
        """全URL削除"""
        if not self.urls:
            return
        
        if messagebox.askyesno("確認", f"{len(self.urls)}個のURLをすべて削除しますか？"):
            self.urls.clear()
            self.url_list.delete(0, tk.END)
            self._update_url_count()
            self._update_url_index_map()
            self._save_targets()
            self._log("すべてのURLを削除", level="WARNING")

    def _import_urls(self) -> None:
        """URLインポート"""
        path = filedialog.askopenfilename(
            title="URLリストを選択",
            filetypes=[("テキスト", "*.txt"), ("JSON", "*.json"), ("すべて", "*.*")]
        )
        if not path:
            return
        
        try:
            content = Path(path).read_text(encoding="utf-8")
            added = 0
            
            if path.endswith(".json"):
                data = json.loads(content)
                if isinstance(data, dict) and "targets" in data:
                    lines = [str(u) for u in data["targets"] if u]
                elif isinstance(data, dict) and "urls" in data:
                    lines = [str(u) for u in data["urls"] if u]
                elif isinstance(data, list):
                    lines = [str(u) for u in data if u]
                else:
                    lines = []
            else:
                lines = content.splitlines()
            
            for line in lines:
                url = self._normalize_url(str(line))
                if url and url not in self.urls:
                    self.urls.append(url)
                    display_text = f"[待機　] {url}"
                    self.url_list.insert(tk.END, display_text)
                    added += 1
            
            self._update_url_count()
            self._update_url_index_map()
            self._save_targets()
            self._log(f"{added}個のURLをインポート", level="SUCCESS")
            
        except Exception as e:
            self._log(f"インポートエラー: {e}", level="ERROR")

    def _update_url_count(self) -> None:
        """URL数表示を更新"""
        self.url_count_label.config(text=f"{len(self.urls)} URLs")

    def _update_url_index_map(self):
        """URL→インデックスマッピングを更新"""
        self.url_index_map.clear()
        for i in range(self.url_list.size()):
            display_text = self.url_list.get(i)
            # 状態テキストを除去してURLのみ取得
            url = display_text.split("] ", 1)[-1] if "] " in display_text else display_text
            self.url_index_map[url] = i

    def _load_targets(self) -> None:
        """targets.jsonを読み込み"""
        data = read_json_safe(TARGETS_FILE) or {}
        urls = data.get("urls", [])
        
        self.urls.clear()
        self.url_list.delete(0, tk.END)
        
        for url in urls:
            normalized = self._normalize_url(url)
            if normalized and normalized not in self.urls:
                self.urls.append(normalized)
                display_text = f"[待機　] {normalized}"
                self.url_list.insert(tk.END, display_text)
        
        self._update_url_count()
        self._update_url_index_map()
        self._log(f"targets.json読み込み: {len(self.urls)}個のURL", level="INFO")

    def _save_targets(self) -> None:
        """targets.jsonを保存"""
        data = {"urls": self.urls, "updated_at": now_iso()}
        if write_json_safe(TARGETS_FILE, data):
            self._log("targets.json保存", level="DEBUG")
        else:
            self._log("targets.json保存失敗", level="ERROR")

    # ---------------------------------------------------------------------
    # Login Management
    # ---------------------------------------------------------------------
    def _update_login_status(self, status: Any) -> None:
        """ログイン状態を更新"""
        if status is True or str(status).lower() == "strong":
            self.login_status_label.config(text="ログイン済み", foreground="green")
            self._log("ログイン状態: 確認済み", level="SUCCESS")
        elif str(status).lower() == "weak":
            self.login_status_label.config(text="再ログイン推奨", foreground="orange")
            self._log("ログイン状態: 再ログイン推奨", level="WARNING")
        else:
            self.login_status_label.config(text="未ログイン", foreground="red")
            self._log(f"ログイン状態: {status}", level="WARNING")

    def _check_login_status_async(self) -> None:
        """ログイン状態確認（非同期）"""
        def worker():
            try:
                if TwitCastingRecorder is None:
                    self.root.after(0, lambda: self._log("facade未検出", level="ERROR"))
                    return
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def check():
                    rec = TwitCastingRecorder()
                    try:
                        return await rec.test_login_status()
                    finally:
                        await rec.close(keep_chrome=False)
                
                status = loop.run_until_complete(check())
                loop.close()
                
                self.root.after(0, lambda: self._update_login_status(status))
                
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda err=error_msg: self._log(f"ログイン確認エラー: {err}", level="ERROR"))
        
        self._thread_pool.submit(worker)

    def _open_login_wizard_async(self) -> None:
        """ログインウィザードを開く（非同期）"""
        def worker():
            try:
                if TwitCastingRecorder is None:
                    self.root.after(0, lambda: self._log("facade未検出", level="ERROR"))
                    return
                
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def wizard():
                    rec = TwitCastingRecorder()
                    try:
                        return await rec.setup_login()
                    finally:
                        await rec.close(keep_chrome=False)
                
                success = loop.run_until_complete(wizard())
                loop.close()
                
                if success:
                    self.root.after(0, lambda: self._log("ログインウィザード完了", level="SUCCESS"))
                    self.root.after(100, self._check_login_status_async)
                else:
                    self.root.after(0, lambda: self._log("ログインウィザードキャンセル", level="WARNING"))
                    
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda err=error_msg: self._log(f"ウィザードエラー: {err}", level="ERROR"))
        
        self._thread_pool.submit(worker)

    # ---------------------------------------------------------------------
    # Monitor Control (完全版)
    # ---------------------------------------------------------------------
    def _start_monitoring(self) -> None:
        """監視開始（完全版）"""
        with self._state_lock:
            if not self.state.can_start():
                self._log(f"開始できません（現在: {self.state.value}）", level="WARNING")
                return
            
            if not self.urls:
                self._log("URLが登録されていません", level="WARNING")
                return
            
            # 既に録画中のURLがあるかチェック
            if self._sessions:
                self._log(f"録画中のセッションがあります: {list(self._sessions.keys())}", level="WARNING")
                return
            
            self.state = GUIState.STARTING
            self.start_btn.config(state="disabled")
            self._stop_call_count = 0
            self._stop_initiated_time = None
            
            try:
                # RecorderWrapper状態をリセット（安全に）
                if RecorderWrapper:
                    for url in self.urls:
                        try:
                            if hasattr(RecorderWrapper, 'set_state'):
                                RecorderWrapper.set_state(url, "idle")
                        except Exception:
                            pass
                
                # Start async loop
                self.loop = asyncio.new_event_loop()
                self.loop_thread = threading.Thread(
                    target=self._loop_worker,
                    daemon=True
                )
                self.loop_thread.start()
                
                # Task manager初期化
                self._task_manager = AsyncTaskManager()
                
                # Load config
                config_data = read_json_safe(ROOT / "config.json") or {}
                monitor_config = config_data.get("monitor", {})
                
                config = MonitorConfig(
                    poll_interval=monitor_config.get("poll_interval", DEFAULT_POLL_INTERVAL),
                    max_concurrent=monitor_config.get("max_concurrent", DEFAULT_MAX_CONCURRENT),
                    root_dir=str(ROOT),
                    urls=self.urls
                )
                
                # Start engine
                self.engine = MonitorEngine(config)
                fut = asyncio.run_coroutine_threadsafe(
                    self._init_and_start_engine(),
                    self.loop
                )
                fut.result(timeout=10)
                
                self.state = GUIState.RUNNING
                self.stop_btn.config(state="normal")
                
                self._log("=== 監視開始 ===", level="SUCCESS", important=True)
                self._schedule_status_refresh()
                
            except Exception as e:
                self._log(f"起動エラー: {e}", level="ERROR")
                self._emergency_cleanup()
                self.state = GUIState.ERROR
                self.start_btn.config(state="normal")

    async def _init_and_start_engine(self) -> None:
        """エンジンを初期化して起動"""
        await self.engine.initialize()
        # Task managerで管理
        if self._task_manager:
            await self._task_manager.create_task("engine_main", self.engine.start)
        else:
            await self.engine.start()

    def _loop_worker(self) -> None:
        """イベントループワーカー（改良版）"""
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
        finally:
            # ループが停止するまで待機
            retry_count = 0
            while self.loop.is_running() and retry_count < 50:
                time.sleep(0.01)
                retry_count += 1
            
            # タスク破棄エラー抑制
            pending = asyncio.all_tasks(self.loop)
            for task in pending:
                task.cancel()
            
            # 回収を試みる
            if pending:
                # 新しいループで回収
                cleanup_loop = asyncio.new_event_loop()
                try:
                    cleanup_loop.run_until_complete(
                        asyncio.wait_for(
                            asyncio.gather(*pending, return_exceptions=True),
                            timeout=2.0
                        )
                    )
                except:
                    pass
                finally:
                    cleanup_loop.close()
            
            # ループクローズ
            if not self.loop.is_closed():
                self.loop.close()

    def _stop_monitoring(self) -> None:
        """監視停止（完全版）"""
        with self._state_lock:
            if not self.state.can_stop():
                self._log(f"停止できません（現在: {self.state.value}）", level="WARNING")
                return
            
            if self._stopping_in_progress:
                self._log("すでに停止処理中", level="WARNING")
                return
            
            # 停止回数チェック
            self._stop_call_count += 1
            if self._stop_call_count > 1:
                self._log(f"停止処理実行済み（{self._stop_call_count}回目の呼び出しを無視）", level="DEBUG")
                return
            
            self._stopping_in_progress = True
            self._stop_initiated_time = time.time()
            self.state = GUIState.STOPPING
            
            # UI即座にロック
            self.stop_btn.config(state="disabled")
            self.start_btn.config(state="disabled")
            self.login_btn.config(state="disabled")
            self.wizard_btn.config(state="disabled")
            self.cookie_btn.config(state="disabled")
            
            self._log("監視を停止しています...", level="WARNING")
            
            # タイムアウトタイマー開始
            self._stop_timeout_id = self.root.after(STOP_TIMEOUT_MS, self._handle_stop_timeout)
        
        # 完全非ブロッキング停止
        def worker():
            futures_to_cancel = []
            
            try:
                if self.engine and self.loop:
                    # エンジン停止
                    stop_future = asyncio.run_coroutine_threadsafe(
                        self._stop_engine_safely(),
                        self.loop
                    )
                    futures_to_cancel.append(stop_future)
                    
                    try:
                        stop_future.result(timeout=10)
                        self._log("エンジン正常停止", level="INFO")
                    except asyncio.TimeoutError:
                        self._log("エンジン停止タイムアウト - 強制キャンセル", level="WARNING")
                    except Exception as e:
                        self._log(f"エンジン停止エラー: {e}", level="WARNING")
                
                # Task manager停止
                if self._task_manager and self.loop:
                    cancel_future = asyncio.run_coroutine_threadsafe(
                        self._task_manager.cancel_all(),
                        self.loop
                    )
                    try:
                        cancelled = cancel_future.result(timeout=5)
                        if cancelled > 0:
                            self._log(f"{cancelled}個のタスクをキャンセル", level="INFO")
                    except:
                        pass
                
                # 追跡プロセスを全て終了
                killed = self._process_manager.kill_all_tracked()
                if killed > 0:
                    self._log(f"{killed}個のプロセスを終了", level="INFO")
                
                # ループ停止
                if self.loop and not self.loop.is_closed():
                    self.loop.call_soon_threadsafe(self.loop.stop)
                
                # UI更新
                self.root.after(0, lambda: self._finalize_stop())
                
            except Exception as e:
                error_msg = str(e)
                self.root.after(0, lambda err=error_msg: self._finalize_stop(error=err))
            finally:
                # 必ずFutureをキャンセル
                for fut in futures_to_cancel:
                    if fut and not fut.done():
                        try:
                            fut.cancel()
                        except:
                            pass
        
        self._thread_pool.submit(worker)

    async def _stop_engine_safely(self) -> None:
        """エンジン安全停止（新メソッド）"""
        if not self.engine:
            return
        
        try:
            # まず録画中のジョブを個別停止
            if hasattr(self.engine, 'active_jobs'):
                active_urls = list(self.engine.active_jobs.keys())
                self._log(f"録画中: {len(active_urls)}件", level="INFO")
                
                for url in active_urls:
                    try:
                        # RecorderWrapper経由で停止状態に
                        if RecorderWrapper:
                            RecorderWrapper.set_state(url, "stopping")
                        
                        # 個別タスクキャンセル
                        if url in self.engine.active_jobs:
                            task = self.engine.active_jobs[url]
                            if isinstance(task, asyncio.Task):
                                task.cancel()
                                
                                # キャンセル完了を短時間待機
                                try:
                                    await asyncio.wait_for(task, timeout=1.0)
                                except (asyncio.TimeoutError, asyncio.CancelledError):
                                    pass
                        
                    except Exception as e:
                        self._log(f"録画停止エラー ({url}): {e}", level="DEBUG")
            
            # エンジン本体の停止
            try:
                await asyncio.wait_for(self.engine.stop(), timeout=5.0)
                self._log("エンジン停止完了", level="INFO")
            except asyncio.TimeoutError:
                self._log("エンジン停止タイムアウト", level="WARNING")
                # 強制的にstop_eventをセット
                if hasattr(self.engine, '_stop_event'):
                    self.engine._stop_event.set()
            
        except Exception as e:
            self._log(f"エンジン停止エラー: {e}", level="ERROR")

    def _handle_stop_timeout(self) -> None:
        """停止タイムアウト処理"""
        with self._state_lock:
            if self.state == GUIState.STOPPING:
                elapsed = time.time() - self._stop_initiated_time if self._stop_initiated_time else 0
                self._log(f"停止処理タイムアウト（{int(elapsed)}秒経過）", level="WARNING")
                
                # 強制終了ボタンを表示
                if self.force_stop_btn:
                    self.force_stop_btn.config(state="normal")
                    self.force_stop_btn.pack(side=tk.LEFT, padx=5)

    def _force_stop_monitoring(self) -> None:
        """強制終了処理"""
        self._log("強制終了を実行します", level="WARNING")
        
        # 全プロセス強制終了
        killed = self._process_manager.kill_all_tracked()
        if killed > 0:
            self._log(f"強制終了: {killed}個のプロセス", level="INFO")
        
        # ループ強制停止
        if self.loop:
            try:
                self.loop.call_soon_threadsafe(self.loop.stop)
            except:
                pass
        
        # RecorderWrapper緊急リセット
        if RecorderWrapper and hasattr(RecorderWrapper, 'emergency_reset'):
            try:
                RecorderWrapper.emergency_reset()
            except:
                pass
        
        # 即座に完了状態へ
        self._finalize_stop(force=True)

    def _finalize_stop(self, error: str = None, force: bool = False) -> None:
        """停止処理を完了"""
        with self._state_lock:
            # タイマーキャンセル
            if self._status_refresh_id:
                with suppress(Exception):
                    self.root.after_cancel(self._status_refresh_id)
                self._status_refresh_id = None
            
            if self._stop_timeout_id:
                with suppress(Exception):
                    self.root.after_cancel(self._stop_timeout_id)
                self._stop_timeout_id = None
            
            # 状態リセット
            self.state = GUIState.IDLE
            self._stopping_in_progress = False
            self._stop_call_count = 0
            self._stop_initiated_time = None
            self._sessions.clear()
            
            # UIロック解除
            self.start_btn.config(state="normal")
            self.stop_btn.config(state="disabled")
            self.login_btn.config(state="normal")
            self.wizard_btn.config(state="normal")
            self.cookie_btn.config(state="normal")
            
            # 強制終了ボタンを隠す
            if self.force_stop_btn:
                self.force_stop_btn.config(state="disabled")
                self.force_stop_btn.pack_forget()
            
            # ループ停止
            if self.loop:
                with suppress(Exception):
                    self.loop.call_soon_threadsafe(self.loop.stop)
            
            # Task managerクリア
            self._task_manager = None
            
            # ログ出力
            if force:
                self._log("=== 強制終了完了 ===", level="WARNING", important=True)
            elif error:
                self._log(f"停止エラー: {error}", level="WARNING")
            else:
                self._log("=== 監視停止 ===", level="SUCCESS", important=True)
            
            # 終了要求処理
            if self._closing_requested:
                self.root.after(100, self.root.destroy)

    def _emergency_cleanup(self) -> None:
        """緊急クリーンアップ"""
        if self.loop and not self.loop.is_closed():
            with suppress(Exception):
                self.loop.call_soon_threadsafe(self.loop.stop)
        
        if self.loop_thread and self.loop_thread.is_alive():
            self.loop_thread.join(timeout=1)
        
        self.engine = None
        self.loop = None
        self.loop_thread = None
        self._task_manager = None

    def _schedule_status_refresh(self) -> None:
        """ステータス更新をスケジュール"""
        if self.state == GUIState.RUNNING:
            self._update_status_display()
            self._status_refresh_id = self.root.after(1000, self._schedule_status_refresh)

    def _update_status_display(self) -> None:
        """ステータス表示を更新"""
        try:
            # heartbeat読み込み
            data = read_json_safe(HEARTBEAT_PRIMARY) or {}
            
            self.status_labels["total_success"].config(text=str(data.get("total_successes", 0)))
            self.status_labels["total_errors"].config(text=str(data.get("total_errors", 0)))
            self.status_labels["last_update"].config(text=now_hms())
            
        except Exception as e:
            print(f"[STATUS_UPDATE_ERROR] {e}")

    # ---------------------------------------------------------------------
    # Process Management (安全版)
    # ---------------------------------------------------------------------
    def _kill_orphans(self) -> None:
        """孤児プロセスを手動で終了（psutil必須）"""
        if not PSUTIL_AVAILABLE:
            self._log("psutilがインストールされていません", level="ERROR")
            return
        
        orphans = self._process_manager.find_orphan_processes()
        if not orphans:
            self._log("孤児プロセスはありません", level="INFO")
            return
        
        if messagebox.askyesno("確認", f"{len(orphans)}個の孤児プロセスを終了しますか？"):
            killed = 0
            for pid in orphans:
                try:
                    proc = psutil.Process(pid)
                    proc.terminate()
                    try:
                        proc.wait(timeout=2)
                    except psutil.TimeoutExpired:
                        proc.kill()
                    killed += 1
                except Exception:
                    pass
            self._log(f"{killed}個の孤児プロセスを終了", level="SUCCESS")

    # ---------------------------------------------------------------------
    # Other functions
    # ---------------------------------------------------------------------
    def _show_logs(self) -> None:
        """ログを表示"""
        files = []
        for log_dir in (LOGS_DIR_PRIMARY, LOGS_DIR_FALLBACK):
            if log_dir.exists():
                files.extend(log_dir.glob("monitor_*.jsonl"))
        
        if not files:
            self._log("ログファイルがありません", level="WARNING")
            return
        
        latest = max(files, key=lambda p: p.stat().st_mtime)
        try:
            lines = latest.read_text(encoding="utf-8").splitlines()[-100:]
            self._log(f"=== {latest.name} (最後の100行) ===", level="INFO")
            for line in lines[:10]:
                try:
                    data = json.loads(line)
                    self._log(f"[{data.get('ts', '')}] {data.get('event', '')}", level="DEBUG")
                except Exception:
                    self._log(line[:100], level="DEBUG")
        except Exception as e:
            self._log(f"ログ読み込みエラー: {e}", level="ERROR")

    def _show_config(self) -> None:
        """設定を表示"""
        config_path = ROOT / "config.json"
        if not config_path.exists():
            self._log("config.jsonが見つかりません", level="WARNING")
            return
        
        try:
            data = read_json_safe(config_path)
            monitor = data.get("monitor", {})
            self._log("=== 設定 ===", level="INFO")
            self._log(f"poll_interval: {monitor.get('poll_interval', DEFAULT_POLL_INTERVAL)}", level="DEBUG")
            self._log(f"max_concurrent: {monitor.get('max_concurrent', DEFAULT_MAX_CONCURRENT)}", level="DEBUG")
            self._log(f"headless: {data.get('headless', True)}", level="DEBUG")
        except Exception as e:
            self._log(f"設定読み込みエラー: {e}", level="ERROR")

    def _log(self, message: str, level: str = "INFO", important: bool = False) -> None:
        """ログ出力（スレッドセーフ版）"""
        timestamp = now_hms()
        prefix = "* " if important else ""
        
        # Console output
        print(f"[{timestamp}] [{level}] {prefix}{message}")
        
        # GUI display
        def _append():
            try:
                self.log_text.insert(tk.END, f"[{timestamp}] {prefix}{message}\n", level)
                self.log_text.see(tk.END)
                # Line limit
                lines = int(self.log_text.index('end-1c').split('.')[0])
                if lines > 1000:
                    self.log_text.delete('1.0', '100.0')
            except Exception as e:
                print(f"[GUI-LOG SUPPRESSED] {e}", file=sys.stderr)
        
        try:
            if threading.current_thread() is threading.main_thread():
                _append()
            else:
                self.root.after(0, _append)
        except Exception as e:
            print(f"[GUI-LOG SCHED SUPPRESSED] {e}", file=sys.stderr)

    def _on_closing(self) -> None:
        """ウィンドウクローズ時"""
        # 監視スレッド停止
        self._gui_state_stop.set()
        
        # タイマー停止
        for timer_id in [self._health_check_id, self._state_poller_id]:
            if timer_id:
                with suppress(Exception):
                    self.root.after_cancel(timer_id)
        
        self._closing_requested = True
        
        # 録画中チェック
        if self._sessions:
            if messagebox.askyesno("確認", f"録画中です（{len(self._sessions)}件）。停止して終了しますか？"):
                self._stop_monitoring()
            return
        
        if self.state == GUIState.RUNNING:
            if messagebox.askyesno("確認", "監視中です。停止して終了しますか？"):
                self._stop_monitoring()
            return
        
        # スレッドプール停止
        self._thread_pool.shutdown(wait=False)
        
        # 全追跡プロセスを終了
        killed = self._process_manager.kill_all_tracked()
        if killed > 0:
            self._log(f"終了時に{killed}個のプロセスを終了", level="INFO")
        
        self.root.destroy()

    def run(self) -> None:
        """GUIを実行"""
        try:
            self.root.mainloop()
        except KeyboardInterrupt:
            self._on_closing()
        finally:
            # クリーンアップ
            with suppress(Exception):
                self._thread_pool.shutdown(wait=False)


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    try:
        app = MonitorGUI()
        app.run()
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        traceback.print_exc()
        
        try:
            root = tk.Tk()
            root.withdraw()
            messagebox.showerror("致命的エラー", f"起動に失敗しました:\n{e}")
            root.destroy()
        except Exception:
            pass
        
        sys.exit(1)