#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TwitCasting Recorder GUI (完璧版)
- 【確認済】ROOT挿入ガード実装済み
- ファイル名の特殊文字対応
- 確実なファイル検出
- 詳細なファイル情報表示
- 重大バグ修正：UI復旧強化、keep_chrome対応
"""

# 【確認済】ROOT挿入ガード（最初に必ず実行）
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncio
import json
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext, ttk
from typing import Optional
import os
from datetime import datetime
import traceback

from facade import (
    TwitCastingRecorder,
    get_config_class,
    get_paths,
    AlreadyRecordingError,
    InitializationError,
)

# Config取得
Config = get_config_class()

# パス取得  
paths = get_paths()
ROOT = paths["ROOT"]
RECORDINGS = paths["RECORDINGS"]
LOGS = paths["LOGS"]

class RecorderGUI:
    def __init__(self) -> None:
        self.root = tk.Tk()
        self.root.title("TwitCasting Recorder v2.1")
        self.root.geometry("920x700")

        self.cfg = Config.load()
        # Async event loop（UIスレッドと分離）
        self.loop = asyncio.new_event_loop()
        self.worker = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.worker.start()

        self.recorder: Optional[TwitCastingRecorder] = None
        self.recording_task: Optional[asyncio.Future] = None
        
        # 追加: 初期化フラグ
        self._initialized = False
        # 追加: ボタン参照を保持（後で有効/無効を切り替えるため）
        self._all_buttons = []

        self._build_ui()
        self._append(f"[INFO] ROOT={ROOT}")
        self._append(f"[INFO] RECORDINGS={RECORDINGS}")
        self._append(f"[INFO] LOGS={LOGS}")
        self._set_status("初期化中...")
        
        # 追加: 終了時のクリーンアップ登録
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        # 初期化（完了時にボタン有効化）
        self._call_async(self._boot(), self._on_boot_complete)

    # ===== UI構築 ======================================================
    def _build_ui(self) -> None:
        # 上段：URL/時間
        top = ttk.Frame(self.root, padding=8)
        top.pack(fill=tk.X)

        ttk.Label(top, text="URL:").grid(row=0, column=0, sticky="w")
        self.url_var = tk.StringVar()
        ttk.Entry(top, textvariable=self.url_var, width=70).grid(row=0, column=1, sticky="we", padx=(4, 8))
        top.grid_columnconfigure(1, weight=1)

        ttk.Label(top, text="秒数(0=無制限):").grid(row=0, column=2, sticky="e")
        self.dur_var = tk.IntVar(value=10)
        ttk.Entry(top, textvariable=self.dur_var, width=10).grid(row=0, column=3, sticky="e")

        # 中段：オプション
        opt = ttk.Frame(self.root, padding=(8, 0))
        opt.pack(fill=tk.X)

        self.headless_var = tk.BooleanVar(value=self.cfg.headless)
        ttk.Checkbutton(opt, text="ヘッドレス(非表示)", variable=self.headless_var, command=self._on_headless_change).pack(side=tk.LEFT)

        ttk.Label(opt, text="FFmpegフォルダ:").pack(side=tk.LEFT, padx=(12, 4))
        self.ffmpeg_var = tk.StringVar(value=self.cfg.ffmpeg_path)
        ttk.Entry(opt, textvariable=self.ffmpeg_var, width=28).pack(side=tk.LEFT)
        
        # ボタンを作成して参照を保持
        btn_browse = ttk.Button(opt, text="参照", command=self._choose_ffmpeg)
        btn_browse.pack(side=tk.LEFT, padx=(4, 0))
        self._all_buttons.append(btn_browse)
        
        btn_save = ttk.Button(opt, text="保存", command=self._save_config)
        btn_save.pack(side=tk.LEFT, padx=6)
        self._all_buttons.append(btn_save)

        # ボタン列
        btn = ttk.Frame(self.root, padding=(8, 10))
        btn.pack(fill=tk.X)
        
        btn_check = ttk.Button(btn, text="ログイン状態確認", command=self._btn_check_login)
        btn_check.pack(side=tk.LEFT, padx=4)
        self._all_buttons.append(btn_check)
        
        btn_setup = ttk.Button(btn, text="ログインセットアップ", command=self._btn_setup_login)
        btn_setup.pack(side=tk.LEFT, padx=4)
        self._all_buttons.append(btn_setup)
        
        btn_test = ttk.Button(btn, text="10秒テスト録画", command=self._btn_quick_test)
        btn_test.pack(side=tk.LEFT, padx=4)
        self._all_buttons.append(btn_test)

        self.stress_btn = ttk.Button(btn, text="短時間ストレス(10秒×2)", command=self._btn_stress_test)
        self.stress_btn.pack(side=tk.LEFT, padx=4)
        self._all_buttons.append(self.stress_btn)

        self.record_btn = ttk.Button(btn, text="録画開始", command=self._btn_record)
        self.record_btn.pack(side=tk.LEFT, padx=4)
        self._all_buttons.append(self.record_btn)

        self.cancel_btn = ttk.Button(btn, text="キャンセル", command=self._btn_cancel, state=tk.DISABLED)
        self.cancel_btn.pack(side=tk.LEFT, padx=4)
        # cancel_btnは初期化時も無効のままなので、_all_buttonsには含めない

        # ログ
        self.log = scrolledtext.ScrolledText(self.root, wrap=tk.WORD, width=100, height=28)
        self.log.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # ステータスバー
        bar = ttk.Frame(self.root)
        bar.pack(fill=tk.X, side=tk.BOTTOM)
        self.status = tk.StringVar(value="")
        ttk.Label(bar, textvariable=self.status).pack(side=tk.LEFT, padx=6)
        self.login_state = tk.StringVar(value="ログイン: 未確認")
        ttk.Label(bar, textvariable=self.login_state).pack(side=tk.RIGHT, padx=6)
        
        # 初期化完了まで全ボタンを無効化
        self._set_buttons_state(tk.DISABLED)

    # ===== UIユーティリティ ============================================
    def _set_status(self, msg: str):
        self.status.set(msg)

    def _append(self, text: str):
        self.log.insert(tk.END, text + "\n")
        self.log.see(tk.END)
    
    def _set_buttons_state(self, state):
        """全ボタンの有効/無効を切り替え"""
        for btn in self._all_buttons:
            try:
                btn.config(state=state)
            except Exception:
                pass  # ボタンが既に破棄されている場合は無視

    def _on_headless_change(self):
        # 初期化前は何もしない
        if not self._initialized:
            return
            
        self.cfg.headless = bool(self.headless_var.get())
        self.cfg.save()
        # recorderが存在する場合のみ反映
        if self.recorder and hasattr(self.recorder, 'session') and self.recorder.session:
            self.recorder.session.cfg = self.cfg
        self._append(f"[SET] headless = {self.cfg.headless}")

    def _choose_ffmpeg(self):
        # 初期化前は何もしない
        if not self._initialized:
            return
            
        d = filedialog.askdirectory(title="FFmpegのbinフォルダ（ffmpeg.exeがある場所）を選択")
        if d:
            self.ffmpeg_var.set(d)
            self.cfg.ffmpeg_path = d
            self.cfg.save()
            # 即時反映
            if self.recorder and hasattr(self.recorder, 'session') and self.recorder.session:
                self.recorder.session.cfg = self.cfg
            self._append(f"[SET] ffmpeg_path = {d}")

    def _save_config(self):
        # 初期化前は何もしない
        if not self._initialized:
            return
            
        self.cfg.ffmpeg_path = self.ffmpeg_var.get().strip()
        self.cfg.save()
        if self.recorder and hasattr(self.recorder, 'session') and self.recorder.session:
            self.recorder.session.cfg = self.cfg
        self._append(f"[SET] ffmpeg_path = {self.cfg.ffmpeg_path or '(未設定)'}")

    # ===== ファイル検出ヘルパー ==============================
    def _find_actual_output_files(self, output_base: str) -> list:
        """
        実際に生成されたファイルを賢く探す
        yt-dlpがWindows禁止文字を置換することを考慮
        """
        if not output_base:
            return []
        
        base_path = Path(output_base)
        parent_dir = base_path.parent
        base_name = base_path.name
        
        # 見つかったファイルのリスト
        found_files = []
        
        # パターン1: そのままのファイル名で検索
        pattern1_files = list(parent_dir.glob(f"{base_name}.*"))
        if pattern1_files:
            self._append(f"[DEBUG] パターン1（そのまま）でファイル発見: {len(pattern1_files)}個")
            found_files.extend(pattern1_files)
            return found_files
        
        # パターン2: Windows禁止文字を置換して検索
        if sys.platform == "win32":
            # yt-dlpの一般的な変換規則
            replacements = [
                (':', '#'),  # 最も一般的（Google認証ユーザー等）
                (':', '_'),  # 代替パターン
                ('*', '_'),
                ('?', '_'),
                ('"', '_'),
                ('<', '_'),
                ('>', '_'),
                ('|', '_'),
                ('/', '_'),
                ('\\', '_'),
            ]
            
            for old_char, new_char in replacements:
                if old_char in base_name:
                    safe_name = base_name.replace(old_char, new_char)
                    pattern2_files = list(parent_dir.glob(f"{safe_name}.*"))
                    if pattern2_files:
                        self._append(f"[DEBUG] パターン2（{old_char}→{new_char}）でファイル発見: {safe_name}")
                        found_files.extend(pattern2_files)
                        if found_files:
                            return found_files
        
        # パターン3: タイムスタンプベースの検索（最終手段）
        try:
            # ファイル名が "YYYYMMDD_HHMMSS_userid_uuid" 形式と仮定
            parts = base_name.split('_')
            if len(parts) >= 4:  # 最低4パーツ必要
                timestamp = f"{parts[0]}_{parts[1]}"  # YYYYMMDD_HHMMSS
                uuid = parts[-1]  # 最後のUUID部分
                
                # タイムスタンプとUUIDが一致するファイルを検索
                pattern3 = f"{timestamp}_*_{uuid}.*"
                pattern3_files = list(parent_dir.glob(pattern3))
                if pattern3_files:
                    self._append(f"[DEBUG] パターン3（タイムスタンプ検索）でファイル発見")
                    found_files.extend(pattern3_files)
        except Exception as e:
            self._append(f"[DEBUG] パターン3検索中にエラー（無視）: {e}")
        
        # 重複を除去して返す
        unique_files = list(set(found_files))
        return sorted(unique_files)  # ソートして返す

    # ===== 非同期呼び出し（重大バグ修正版） ==============================================
    def _call_async(self, coro, on_done):
        """
        非同期タスク実行（エラーハンドリング強化）
        """
        async def runner():
            try:
                return await coro
            except Exception as e:
                return e

        def cb(task):
            def ui_update():
                try:
                    res = task.result()
                    if isinstance(res, Exception):
                        # 例外の場合もdictに変換してon_doneに渡す
                        error_dict = {"success": False, "error": str(res)}
                        self._append(f"[ERROR] {res}")
                        if self._initialized:
                            # 初期化後のみエラーダイアログ表示
                            # ただしAlreadyRecordingErrorは除く
                            if not isinstance(res, AlreadyRecordingError):
                                messagebox.showerror("エラー", str(res))
                        on_done(error_dict)
                    else:
                        on_done(res)
                except Exception as e:
                    # コールバック自体のエラー
                    self._append(f"[ERROR] Callback error: {e}")
                    # エラー辞書を作ってon_doneを呼ぶ
                    try:
                        on_done({"success": False, "error": f"callback_error: {e}"})
                    except:
                        pass
            
            # root.after(0, ...)でメインスレッドにディスパッチ
            try:
                self.root.after(0, ui_update)
            except Exception as e:
                self._append(f"[ERROR] UI dispatch error: {e}")

        try:
            fut = asyncio.run_coroutine_threadsafe(runner(), self.loop)
            fut.add_done_callback(cb)
            return fut
        except Exception as e:
            self._append(f"[ERROR] Task scheduling error: {e}")
            return None

    async def _boot(self):
        """初期化処理"""
        self.recorder = TwitCastingRecorder()
        await self.recorder.initialize()
        return True
    
    def _on_boot_complete(self, result):
        """初期化完了時の処理"""
        if isinstance(result, dict) and not result.get("success", True):
            # エラーの場合
            self._append(f"[SYSTEM] 初期化失敗: {result.get('error', 'unknown')}")
            self._set_status("初期化失敗")
            messagebox.showerror("初期化エラー", "レコーダーの初期化に失敗しました")
        elif result is True:
            self._initialized = True
            self._set_buttons_state(tk.NORMAL)  # ボタン有効化
            self._append("[SYSTEM] 初期化完了")
            self._set_status("準備完了")
        else:
            self._append("[SYSTEM] 初期化失敗")
            self._set_status("初期化失敗")
            messagebox.showerror("初期化エラー", "レコーダーの初期化に失敗しました")
    
    def _on_close(self):
        """ウィンドウ終了時のクリーンアップ（keep_chrome対応）"""
        try:
            # 録画中の場合はキャンセル
            if self.recording_task and not self.recording_task.done():
                self.recording_task.cancel()
            
            # レコーダーのクリーンアップ（GUI終了時は完全終了）
            if self.recorder:
                fut = asyncio.run_coroutine_threadsafe(
                    self.recorder.close(keep_chrome=False),  # GUI終了時は完全終了
                    self.loop
                )
                try:
                    fut.result(timeout=3)
                except Exception:
                    pass
            
            # イベントループ停止
            self.loop.call_soon_threadsafe(self.loop.stop)
            
        except Exception as e:
            print(f"Cleanup error: {e}")
        finally:
            self.root.destroy()

    # ===== ボタンハンドラ ==============================================
    def _btn_check_login(self):
        # 初期化チェック
        if not self._initialized or not self.recorder:
            return
            
        self._set_status("ログイン状態確認中…")
        async def work():
            status = await self.recorder.test_login_status()
            return status
        def done(status):
            if isinstance(status, dict) and not status.get("success", True):
                # エラーの場合
                self.login_state.set("ログイン: エラー")
                self._append(f"[LOGIN] エラー: {status.get('error', 'unknown')}")
            else:
                # statusは "strong", "weak", "none" のいずれか
                if status == "strong":
                    self.login_state.set("ログイン: 済（強）")
                elif status == "weak":
                    self.login_state.set("ログイン: 済（弱）")
                else:
                    self.login_state.set("ログイン: 未")
                self._append(f"[LOGIN] 状態 = {status}")
            self._set_status("完了")
        self._call_async(work(), done)

    def _btn_setup_login(self):
        # 初期化チェック
        if not self._initialized or not self.recorder:
            return
            
        self._set_status("ログインセットアップ起動…（ブラウザで手動ログイン）")
        # 強制ヘッドありで実施 → 成功時のみ元設定に戻す
        prev_headless = self.headless_var.get()
        self.cfg.headless = False
        self.cfg.save()
        if self.recorder and hasattr(self.recorder, 'session') and self.recorder.session:
            self.recorder.session.cfg = self.cfg

        async def work():
            try:
                # Chrome可視化を確実にするため、一度リセット
                await self.recorder.close(keep_chrome=True)  # セッション維持
                await self.recorder.initialize()
                
                ok = await self.recorder.setup_login()
                
                # 成功時のみ UI のチェックに従って戻す
                if ok:
                    self.cfg.headless = bool(prev_headless)
                    self.cfg.save()
                    if self.recorder and hasattr(self.recorder, 'session') and self.recorder.session:
                        self.recorder.session.cfg = self.cfg
                return ok
            except Exception as e:
                # エラー時も設定を戻す
                self.cfg.headless = bool(prev_headless)
                self.cfg.save()
                raise e

        def done(result):
            if isinstance(result, dict) and not result.get("success", True):
                # エラーの場合
                self.login_state.set("ログイン: 失敗")
                self._append(f"[LOGIN] セットアップエラー: {result.get('error', 'unknown')}")
            else:
                ok = bool(result)
                self.login_state.set("ログイン: 済" if ok else "ログイン: 未")
                self._append(f"[LOGIN] セットアップ結果 = {ok}")
            self._set_status("完了")

        self._call_async(work(), done)

    def _btn_quick_test(self):
        # 初期化チェック
        if not self._initialized or not self.recorder:
            return
            
        if not self.url_var.get().strip():
            self.url_var.set("https://twitcasting.tv/username/broadcaster")
        self.dur_var.set(10)
        self._btn_record()

    def _btn_stress_test(self):
        """
        10秒×2回の連続テスト（修正版：確実な完了待機）
        """
        # 初期化チェック
        if not self._initialized or not self.recorder:
            return
        
        # 実行中チェック
        if self.stress_btn['state'] == tk.DISABLED:
            self._append("[DEBUG] ストレステストは既に実行中です")
            return
            
        url = self.url_var.get().strip() or "https://twitcasting.tv/username/broadcaster"
        self.url_var.set(url)
        
        # UI無効化（全ボタン）
        self._set_status("ストレステスト実行中...")
        self.stress_btn.config(state=tk.DISABLED)
        self.record_btn.config(state=tk.DISABLED)
        self.cancel_btn.config(state=tk.DISABLED)

        async def work():
            results = []
            for i in range(2):
                self._append(f"[STRESS] テスト {i+1}/2 開始...")
                
                # Core側のフラグが解放されるまで待機
                retry_count = 0
                while hasattr(self.recorder, 'is_recording') and self.recorder.is_recording:
                    if retry_count > 20:  # 10秒待っても解放されない
                        self._append(f"[STRESS] 録画フラグが解放されません")
                        results.append({"success": False, "error": "timeout_waiting_flag"})
                        break
                    await asyncio.sleep(0.5)
                    retry_count += 1
                
                if retry_count <= 20:
                    try:
                        # 少し待機（安定性向上）
                        await asyncio.sleep(0.5)
                        res = await self.recorder.record(url, duration=10)
                        results.append(res)
                        
                        if res.get("success"):
                            self._append(f"[STRESS] テスト {i+1}/2 成功")
                        else:
                            self._append(f"[STRESS] テスト {i+1}/2 失敗: {res.get('error')}")
                        
                        # 次の録画まで確実に待つ
                        if i < 1:  # 最後の録画後は待たない
                            self._append("[STRESS] 3秒待機中...")
                            await asyncio.sleep(3)
                            
                    except Exception as e:
                        self._append(f"[STRESS] テスト {i+1}/2 エラー: {e}")
                        results.append({"success": False, "error": str(e)})
                    
            return results

        def done(results):
            # UI復活（全ボタン）
            self.stress_btn.config(state=tk.NORMAL)
            self.record_btn.config(state=tk.NORMAL)
            self.cancel_btn.config(state=tk.DISABLED)  # キャンセルは無効のまま
            
            if isinstance(results, dict) and not results.get("success", True):
                # エラーの場合
                self._append(f"[STRESS] エラー: {results.get('error', 'unknown')}")
                self._set_status("ストレステストエラー")
                messagebox.showerror("ストレステスト", "エラーが発生しました")
            else:
                ok_count = sum(1 for r in results if r.get("success"))
                self._append(f"[STRESS] 完了: 成功 {ok_count}/2")
                
                if ok_count < 2:
                    self._append("[STRESS] 失敗詳細:")
                    for i, r in enumerate(results):
                        if not r.get("success"):
                            self._append(f"  テスト{i+1}: {r.get('error', 'unknown')}")
                
                self._set_status("ストレステスト完了")
                messagebox.showinfo("ストレステスト", f"成功 {ok_count}/2")
        
        self._call_async(work(), done)

    def _btn_record(self):
        """
        録画開始（重大バグ修正版：例外時のUI復旧強化）
        """
        # 初期化チェック
        if not self._initialized or not self.recorder:
            return
        
        # ボタン連打対策
        if self.record_btn['state'] == tk.DISABLED:
            self._append("[DEBUG] 録画ボタンは既に無効化されています")
            return
        
        # 即座にボタン無効化
        self.record_btn.config(state=tk.DISABLED)
        self.cancel_btn.config(state=tk.NORMAL)
            
        url = self.url_var.get().strip()
        if not url:
            # URLが空の場合はボタンを戻す
            self.record_btn.config(state=tk.NORMAL)
            self.cancel_btn.config(state=tk.DISABLED)
            messagebox.showwarning("URL未指定", "ブロードキャスターURLを入力してください。")
            return
        duration = int(self.dur_var.get() or 0)

        # 多重起動の即時ブロック
        if self.recording_task and not self.recording_task.done():
            self._append("[WARN] 既に録画中のタスクがあります")
            # ボタンは無効のまま（録画終了時に戻る）
            return
        
        # Core側のフラグもチェック
        if hasattr(self.recorder, 'is_recording') and self.recorder.is_recording:
            self._append("[WARN] レコーダーが録画中です")
            # ボタンは無効のまま（録画終了時に戻る）
            return

        self._set_status("録画中…")

        async def work():
            try:
                await asyncio.sleep(0.1)  # 連打対策
                return await self.recorder.record(url, duration=(duration or None))
            except asyncio.CancelledError:
                return {"success": False, "error": "cancelled"}
            except Exception as e:
                self._append(f"[ERROR] 録画エラー: {e}")
                self._append(f"[DEBUG] {traceback.format_exc()}")
                return {"success": False, "error": str(e)}
            finally:
                # 最終保険：done()が呼ばれない場合でもUI復旧
                try:
                    # UIスレッドで実行するため、root.afterを使用
                    def restore_ui():
                        try:
                            if self.record_btn.winfo_exists():
                                if self.record_btn['state'] == tk.DISABLED:
                                    self.record_btn.config(state=tk.NORMAL)
                                if self.cancel_btn.winfo_exists():
                                    self.cancel_btn.config(state=tk.DISABLED)
                        except Exception:
                            pass
                    self.root.after(100, restore_ui)
                except Exception:
                    pass

        def done(res: dict):
            try:
                actual_files = []
                
                # UI戻し（確実に実行）
                try:
                    self.record_btn.config(state=tk.NORMAL)
                    self.cancel_btn.config(state=tk.DISABLED)
                except Exception as ui_error:
                    self._append(f"[ERROR] UI復旧エラー: {ui_error}")
                
                # 結果処理
                self._append(f"[RESULT] success={res.get('success')} code={res.get('code')} elapsed={res.get('elapsed')}s")
                self._append(f"[RESULT] m3u8={res.get('m3u8')}")
                
                # ファイル検出処理
                if res.get("output_base"):
                    output_base = res.get("output_base")
                    self._append(f"[RESULT] 出力ベース: {output_base}")
                    
                    actual_files = self._find_actual_output_files(output_base)
                    
                    if actual_files:
                        self._append(f"[SUCCESS] 録画ファイル生成完了 ({len(actual_files)}個):")
                        for f in actual_files:
                            try:
                                size_bytes = f.stat().st_size
                                size_mb = size_bytes / (1024 * 1024)
                                self._append(f"  ✓ {f.name} ({size_mb:.2f} MB)")
                            except:
                                self._append(f"  ✓ {f.name}")
                    elif res.get("success"):
                        # 録画は成功したがファイルが見つからない場合
                        self._append("[WARN] 録画は成功しましたが、ファイルの場所を特定できません")
                        self._append(f"[INFO] recordingsフォルダを確認してください: {RECORDINGS}")
                        # 最新ファイルを表示（ヒントとして）
                        try:
                            latest_files = sorted(
                                RECORDINGS.glob("*.mp4"),
                                key=lambda x: x.stat().st_mtime,
                                reverse=True
                            )[:3]  # 最新3個
                            if latest_files:
                                self._append("[HINT] 最近作成されたファイル:")
                                for f in latest_files:
                                    mtime = datetime.fromtimestamp(f.stat().st_mtime)
                                    self._append(f"  - {f.name} ({mtime.strftime('%H:%M:%S')})")
                        except Exception as e:
                            self._append(f"[DEBUG] 最新ファイル取得エラー: {e}")
                
                # yt-dlpのログ表示
                self._append("----- yt-dlp tail -----")
                for line in res.get("tail", [])[-40:]:
                    self._append(line)
                self._append("-----------------------")

                # エラーハンドリング
                if not res.get("success"):
                    err = res.get("error", "unknown")
                    self._append(f"[ERROR] 録画失敗: {err}")
                    if err == "already_recording":
                        self._append("[INFO] 既に録画中です。終了を待つか、キャンセルしてください。")
                    elif err != "cancelled":
                        messagebox.showerror("録画失敗", f"エラー: {err}")
                else:
                    # 成功時の処理
                    if not actual_files and res.get("output_base"):
                        # ファイルが見つからないが成功の場合は警告のみ
                        self._append("[INFO] ファイル名に特殊文字が含まれている可能性があります")

                self._set_status("完了")
                
            except Exception as e:
                # コールバックエラーの詳細出力
                tb = traceback.format_exc()
                self._append(f"[ERROR] Callback error: {e}")
                self._append(f"[DEBUG] {tb}")
                # UIは念のため復活させる
                try:
                    self.record_btn.config(state=tk.NORMAL)
                    self.cancel_btn.config(state=tk.DISABLED)
                    self._set_status("エラー")
                except:
                    pass

        # 非同期タスク起動（例外対策強化）
        try:
            self.recording_task = self._call_async(work(), done)
            if not self.recording_task:
                raise RuntimeError("_call_async failed to return task")
        except Exception as e:
            # スケジューリングに失敗した場合でもUIを確実に戻す
            self._append(f"[ERROR] 非同期起動に失敗: {e}")
            self.record_btn.config(state=tk.NORMAL)
            self.cancel_btn.config(state=tk.DISABLED)
            self._set_status("エラー")
            return

    def _btn_cancel(self):
        if self.recording_task and not self.recording_task.done():
            self.recording_task.cancel()
            self._append("[INFO] 録画をキャンセルしました")
            # UIを即時戻す（doneコールバックでも最終調整される）
            self.record_btn.config(state=tk.NORMAL)
            self.cancel_btn.config(state=tk.DISABLED)
            self._set_status("キャンセル済")

    # ===== 入口 ========================================================
    def run(self):
        self.root.mainloop()


if __name__ == "__main__":
    RecorderGUI().run()