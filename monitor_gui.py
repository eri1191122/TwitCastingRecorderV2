#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor GUI for TwitCasting Recorder
監視専用GUI
重大バグ修正版
- イベントループ管理修正
- thread-safe操作
- ディレクトリ作成保証
"""
import asyncio
import json
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import threading
import sys

# Windows EventLoop設定（最初に実行）
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 親ディレクトリをパスに追加
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# パス定義と初期化
TARGETS_FILE = ROOT / "auto" / "targets.json"
if not TARGETS_FILE.exists():
    TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TARGETS_FILE.write_text('{"urls": [], "updated_at": null}', encoding="utf-8")

HEARTBEAT_FILE = ROOT / "logs" / "heartbeat.json"
HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)  # logs/作成保証
CONFIG_FILE = ROOT / "config.json"

from auto.monitor_engine import MonitorEngine
from auto.live_detector import LiveDetector

class MonitorGUI:
    """監視専用GUI（軽量版）"""
    
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("TwitCasting Monitor v1.0")
        self.root.geometry("800x600")
        
        # アイコン設定（あれば）
        try:
            self.root.iconbitmap(ROOT / "icon.ico")
        except Exception:
            pass
            
        # 監視エンジン
        self.engine = None
        self.monitor_task = None
        self.loop = None
        self.thread = None
        
        # GUI要素
        self.target_listbox = None
        self.status_labels = {}
        self.log_text = None
        
        # 状態
        self.is_monitoring = False
        
        # GUI構築
        self._create_widgets()
        self._load_targets()
        self._update_status()
        
        # 定期更新（1秒ごと）
        self._schedule_update()
        
    def _normalize_url(self, url: str) -> str:
        """URL正規化（Engine/CLI/Detectorと統一）"""
        u = str(url).strip()
        if u.startswith(("c:", "g:", "ig:")):
            return f"https://twitcasting.tv/{u}"
        if u.startswith(("http://", "https://")):
            return u
        return f"https://twitcasting.tv/{u}"
        
    def _create_widgets(self):
        """GUI要素作成"""
        
        # メインフレーム
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # 設定エリア（上部）
        config_frame = ttk.LabelFrame(main_frame, text="設定", padding="10")
        config_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=(0, 10))
        
        # 監視ON/OFFチェックボックス
        self.enable_var = tk.BooleanVar()
        self.enable_check = ttk.Checkbutton(
            config_frame,
            text="監視機能を有効化",
            variable=self.enable_var,
            command=self._toggle_monitoring_config
        )
        self.enable_check.grid(row=0, column=0, sticky=tk.W)
        
        # ステータス表示
        status_frame = ttk.Frame(config_frame)
        status_frame.grid(row=0, column=1, padx=(20, 0))
        
        self.status_labels["phase"] = ttk.Label(status_frame, text="状態: 停止中")
        self.status_labels["phase"].grid(row=0, column=0)
        
        self.status_labels["active"] = ttk.Label(status_frame, text="録画中: 0")
        self.status_labels["active"].grid(row=0, column=1, padx=(10, 0))
        
        self.status_labels["disk"] = ttk.Label(status_frame, text="空き: --GB")
        self.status_labels["disk"].grid(row=0, column=2, padx=(10, 0))
        
        # 左側：ターゲットリスト
        left_frame = ttk.Frame(main_frame)
        left_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        ttk.Label(left_frame, text="監視対象URL:").grid(row=0, column=0, sticky=tk.W)
        
        # リストボックス
        list_frame = ttk.Frame(left_frame)
        list_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        scrollbar = ttk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.target_listbox = tk.Listbox(
            list_frame,
            yscrollcommand=scrollbar.set,
            width=40,
            height=15
        )
        self.target_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.config(command=self.target_listbox.yview)
        
        # URL入力
        input_frame = ttk.Frame(left_frame)
        input_frame.grid(row=2, column=0, sticky=(tk.W, tk.E), pady=(10, 0))
        
        self.url_entry = ttk.Entry(input_frame, width=30)
        self.url_entry.grid(row=0, column=0, sticky=(tk.W, tk.E))
        self.url_entry.bind("<Return>", lambda e: self._add_url())
        
        ttk.Button(input_frame, text="追加", command=self._add_url).grid(row=0, column=1, padx=(5, 0))
        ttk.Button(input_frame, text="削除", command=self._remove_url).grid(row=0, column=2, padx=(5, 0))
        
        # 右側：コントロール＋ログ
        right_frame = ttk.Frame(main_frame)
        right_frame.grid(row=1, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=(10, 0))
        
        # コントロールボタン
        control_frame = ttk.Frame(right_frame)
        control_frame.grid(row=0, column=0, sticky=(tk.W, tk.E), pady=(0, 10))
        
        self.start_button = ttk.Button(
            control_frame,
            text="監視開始",
            command=self._start_monitoring,
            state=tk.DISABLED
        )
        self.start_button.grid(row=0, column=0, padx=(0, 5))
        
        self.stop_button = ttk.Button(
            control_frame,
            text="監視停止",
            command=self._stop_monitoring,
            state=tk.DISABLED
        )
        self.stop_button.grid(row=0, column=1, padx=(0, 5))
        
        ttk.Button(
            control_frame,
            text="状態確認",
            command=self._check_selected
        ).grid(row=0, column=2)
        
        # ログエリア
        log_frame = ttk.LabelFrame(right_frame, text="ログ", padding="5")
        log_frame.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        self.log_text = scrolledtext.ScrolledText(
            log_frame,
            width=40,
            height=20,
            wrap=tk.WORD
        )
        self.log_text.pack(fill=tk.BOTH, expand=True)
        
        # グリッド設定
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)
        left_frame.rowconfigure(1, weight=1)
        right_frame.rowconfigure(1, weight=1)
        
    def _load_config(self) -> Dict:
        """設定読み込み"""
        try:
            return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
            
    def _save_config(self, config: Dict):
        """設定保存"""
        try:
            import tempfile
            temp_file = CONFIG_FILE.with_suffix(".tmp")
            temp_file.write_text(
                json.dumps(config, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            temp_file.replace(CONFIG_FILE)
        except Exception as e:
            self._log(f"設定保存エラー: {e}")
            
    def _toggle_monitoring_config(self):
        """監視ON/OFF切り替え"""
        config = self._load_config()
        config["enable_monitoring"] = self.enable_var.get()
        self._save_config(config)
        
        if self.enable_var.get():
            self.start_button.config(state=tk.NORMAL)
            self._log("監視機能: 有効化")
        else:
            self.start_button.config(state=tk.DISABLED)
            if self.is_monitoring:
                self._stop_monitoring()
            self._log("監視機能: 無効化")
            
    def _load_targets(self):
        """ターゲット読み込み"""
        if not TARGETS_FILE.exists():
            TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
            TARGETS_FILE.write_text('{"urls": []}', encoding="utf-8")
            
        try:
            data = json.loads(TARGETS_FILE.read_text(encoding="utf-8"))
            self.target_listbox.delete(0, tk.END)
            for url in data.get("urls", []):
                self.target_listbox.insert(tk.END, url)
                
            # 設定も読み込み
            config = self._load_config()
            self.enable_var.set(config.get("enable_monitoring", False))
            if self.enable_var.get():
                self.start_button.config(state=tk.NORMAL)
        except Exception as e:
            self._log(f"ターゲット読み込みエラー: {e}")
            
    def _save_targets(self):
        """ターゲット保存（atomic write）"""
        urls = list(self.target_listbox.get(0, tk.END))
        data = {
            "urls": urls,
            "updated_at": datetime.now().isoformat()
        }
        
        try:
            TARGETS_FILE.parent.mkdir(parents=True, exist_ok=True)
            temp_file = TARGETS_FILE.with_suffix(".tmp")
            temp_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            temp_file.replace(TARGETS_FILE)
        except Exception as e:
            self._log(f"保存エラー: {e}")
            
    def _add_url(self):
        """URL追加（正規化）"""
        url = self.url_entry.get().strip()
        if not url:
            return
            
        # 正規化
        normalized = self._normalize_url(url)
            
        # 重複チェック
        existing = list(self.target_listbox.get(0, tk.END))
        if normalized not in existing:
            self.target_listbox.insert(tk.END, normalized)
            self._save_targets()
            self._log(f"追加: {normalized}")
            
        self.url_entry.delete(0, tk.END)
        
    def _remove_url(self):
        """URL削除"""
        selection = self.target_listbox.curselection()
        if not selection:
            return
            
        url = self.target_listbox.get(selection[0])
        self.target_listbox.delete(selection[0])
        self._save_targets()
        self._log(f"削除: {url}")
        
    def _check_selected(self):
        """選択URLの状態確認（イベントループ確実にclose）"""
        selection = self.target_listbox.curselection()
        if not selection:
            messagebox.showinfo("確認", "URLを選択してください")
            return
            
        url = self.target_listbox.get(selection[0])
        self._log(f"状態確認: {url}")
        
        # 非同期で確認
        def check_async():
            async def check():
                async with LiveDetector() as detector:
                    return await detector.check_live_status(url)
                    
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(check())
            finally:
                loop.close()  # 必ずclose
            
            # GUI更新
            self.root.after(0, lambda: self._show_check_result(url, result))
            
        threading.Thread(target=check_async, daemon=True).start()
        
    def _show_check_result(self, url: str, result: Dict):
        """確認結果表示"""
        if result.get("is_live"):
            status = f"🔴 配信中\nタイトル: {result.get('title', 'Unknown')}\n視聴者: {result.get('viewers', 0)}"
        else:
            status = f"⚫ オフライン\nエラー: {result.get('error_code', 'NOT_LIVE')}"
            
        messagebox.showinfo(f"状態確認: {url}", status)
        
    def _start_monitoring(self):
        """監視開始"""
        if self.is_monitoring:
            return
            
        self.is_monitoring = True
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        self._log("=== 監視開始 ===")
        
        # 非同期スレッドで監視
        def run_monitor():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.engine = MonitorEngine()
            
            try:
                self.monitor_task = self.loop.create_task(self.engine.watch_and_record())
                self.loop.run_until_complete(self.monitor_task)
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.root.after(0, lambda: self._log(f"エラー: {e}"))
            finally:
                self.loop.close()
                
        self.thread = threading.Thread(target=run_monitor, daemon=True)
        self.thread.start()
        
    def _stop_monitoring(self):
        """監視停止（thread-safe・同一イベントループ）"""
        if not self.is_monitoring:
            return
            
        self.is_monitoring = False
        self.stop_button.config(state=tk.DISABLED)
        self.start_button.config(state=tk.NORMAL if self.enable_var.get() else tk.DISABLED)
        self._log("=== 監視停止中 ===")
        
        # 監視スレッドのループで安全に停止
        if self.engine and self.loop:
            try:
                fut = asyncio.run_coroutine_threadsafe(self.engine.stop(), self.loop)
                fut.result(timeout=15)
            except Exception as e:
                self._log(f"停止エラー: {e}")
                
            # タスクをthread-safeにキャンセル
            if self.monitor_task and not self.monitor_task.done():
                self.loop.call_soon_threadsafe(self.monitor_task.cancel)
                
        # 監視スレッドの終了待機
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
            
        self._log("=== 監視停止完了 ===")
        
    def _update_status(self):
        """ステータス更新"""
        if HEARTBEAT_FILE.exists():
            try:
                hb = json.loads(HEARTBEAT_FILE.read_text(encoding="utf-8"))
                
                # フェーズ
                phase = hb.get("phase", "stopped")
                phase_text = {
                    "monitoring": "🟢 監視中",
                    "stopped": "⚫ 停止中"
                }.get(phase, phase)
                self.status_labels["phase"].config(text=f"状態: {phase_text}")
                
                # アクティブジョブ
                active = hb.get("active_jobs", 0)
                self.status_labels["active"].config(text=f"録画中: {active}")
                
                # ディスク
                disk = hb.get("disk_free_gb", 0)
                self.status_labels["disk"].config(text=f"空き: {disk:.1f}GB")
                
                # 録画中URL表示
                if hb.get("active_urls"):
                    for url in hb["active_urls"]:
                        # リストボックスで該当URLをハイライト
                        for i in range(self.target_listbox.size()):
                            item = self.target_listbox.get(i)
                            if url in item:
                                self.target_listbox.itemconfig(i, bg="lightgreen")
                            else:
                                self.target_listbox.itemconfig(i, bg="white")
                                
            except Exception:
                pass
                
    def _schedule_update(self):
        """定期更新スケジュール"""
        self._update_status()
        self.root.after(1000, self._schedule_update)
        
    def _log(self, msg: str):
        """ログ表示"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {msg}\n")
        self.log_text.see(tk.END)
        
    def run(self):
        """GUI実行"""
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()
        
    def _on_close(self):
        """終了処理"""
        if self.is_monitoring:
            if messagebox.askyesno("確認", "監視中です。停止してから終了しますか？"):
                self._stop_monitoring()
                self.root.after(1000, self.root.destroy)
            else:
                return
        else:
            self.root.destroy()

def main():
    gui = MonitorGUI()
    gui.run()

if __name__ == "__main__":
    main()