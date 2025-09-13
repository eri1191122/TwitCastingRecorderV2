#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Live Detector for TwitCasting
配信検知モジュール（3段階フォールバック版）
Version: 5.2.0

修正内容:
- Streamlink Cookie修正（--http-header使用）
- StreamlinkにUA/Referer追加
- エラー詳細の取得強化
- 既存機能は全て維持
"""
import json
import re
import sys
import os
import time
import logging
import shutil
import asyncio
from typing import Dict, Optional, Any
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from pathlib import Path

# ロガー設定
logger = logging.getLogger("live_detector")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)

# 定数
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
COOKIES_DIR = Path(__file__).resolve().parents[1] / "logs"
READ_SIZE = 512_000  # 512KB読取

# デバッグ用HTMLダンプパス
DEBUG_HTML_PATH = COOKIES_DIR / "debug_live_detector.html"


class LiveDetector:
    """配信状態検知（3段階フォールバック）"""
    
    def __init__(self):
        self.timeout = 10  # HTTP timeout
        self.browser_timeout = 20  # Browser timeout
        self.streamlink_timeout = 10  # Streamlink timeout
        self._cookie_repair_attempted = False
        self._chrome = None  # 遅延初期化用
        self._debug_mode = os.environ.get("DEBUG_LIVE_DETECTOR", "").lower() == "true"
    
    def _normalize_url(self, url: str) -> str:
        """
        URL正規化（TwitCasting仕様準拠）
        g:/ig:プレフィックスはパスに残す
        """
        if not url:
            return ""
        
        url = url.strip()
        
        # プレフィックス対応（TwitCastingの仕様に合わせる）
        prefixes = ("c:", "g:", "ig:", "f:", "tw:")
        for pre in prefixes:
            if url.lower().startswith(pre):
                # プレフィックス後の部分を取得
                name = url[len(pre):].strip()
                if not name:
                    return ""
                
                # g:/ig: はパスにプレフィックスを残す（公式仕様）
                if pre in ("g:", "ig:"):
                    return f"https://twitcasting.tv/{pre}{name}"
                
                # c:/f:/tw: は従来どおり素のユーザ名
                return f"https://twitcasting.tv/{name}"
        
        # /broadcasterを削除
        url = re.sub(r"/broadcaster/?$", "", url)
        
        # httpsスキーム確保
        if not url.startswith("http"):
            url = f"https://twitcasting.tv/{url}"
        
        return url.rstrip("/")
    
    def _latest_enter_cookie_path(self) -> Optional[str]:
        """最新のcookies_enter_*.txt取得"""
        try:
            # latest_cookie_path.txt を最優先
            latest = COOKIES_DIR / "latest_cookie_path.txt"
            if latest.exists():
                p = Path(latest.read_text(encoding="utf-8").strip())
                if p.exists():
                    return str(p)
            
            # フォールバック：mtimeソート
            files = sorted(
                COOKIES_DIR.glob("cookies_enter_*.txt"),
                key=os.path.getmtime,
                reverse=True
            )
            return str(files[0]) if files else None
        except Exception:
            return None
    
    def _check_cookie_integrity(self, path: str) -> Dict[str, bool]:
        """Cookieファイルの完全性チェック"""
        result = {
            "exists": False,
            "has_tc_id": False,
            "has_tc_ss": False,
            "has_session": False,
            "is_complete": False
        }
        
        if not path or not os.path.exists(path):
            return result
        
        result["exists"] = True
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
                result["has_tc_id"] = "tc_id" in content
                result["has_tc_ss"] = "tc_ss" in content
                result["has_session"] = "_twitcasting_session" in content
                result["is_complete"] = result["has_tc_id"] and (
                    result["has_tc_ss"] or result["has_session"]
                )
        except Exception as e:
            logger.error(f"Cookie integrity check failed: {e}")
        
        return result
    
    def _build_cookie_header_from_netscape(self, path: str) -> str:
        """Netscape形式からCookieヘッダ構築"""
        if not path or not os.path.exists(path):
            return ""
        
        wanted = {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#") or line.count("\t") < 6:
                        continue
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) >= 7:
                        name, value = parts[-2], parts[-1]
                        # 重要なCookieを全て収集
                        if name in ("tc_id", "tc_ss", "_twitcasting_session", 
                                   "keep", "mfadid", "did", "tc_s", "tc_u"):
                            wanted[name] = value
        except Exception as e:
            logger.error(f"Cookie parse error: {e}")
            return ""
        
        items = []
        # 認証強度の高い順で入れる
        if "_twitcasting_session" in wanted:
            items.append(f'_twitcasting_session={wanted["_twitcasting_session"]}')
        if "tc_ss" in wanted:
            items.append(f'tc_ss={wanted["tc_ss"]}')
        if "tc_s" in wanted:
            items.append(f'tc_s={wanted["tc_s"]}')
        if "tc_id" in wanted:
            items.append(f'tc_id={wanted["tc_id"]}')
        if "tc_u" in wanted:
            items.append(f'tc_u={wanted["tc_u"]}')
        # その他のCookie
        for name in ("keep", "mfadid", "did"):
            if name in wanted:
                items.append(f'{name}={wanted[name]}')
        
        return "; ".join(items)
    
    def _extract_movie_id(self, html: str) -> Optional[str]:
        """HTMLからmovie_id抽出（複数パターン対応）"""
        patterns = [
            r'data-movie-id="(\d+)"',
            r'"movie_id"\s*:\s*(\d+)',
            r"movieId:\s*'(\d+)'",
            r'movie_id=(\d+)',
            r'data-movie-id=["\'](\d+)["\']',
        ]
        
        for pattern in patterns:
            m = re.search(pattern, html, re.IGNORECASE)
            if m:
                return m.group(1)
        
        return None
    
    def _check_status_http(self, url: str) -> Dict:
        """HTTPベースの配信状態チェック（既存・高速）"""
        # 注：urlは既に正規化済みのものが渡される前提
        if not url:
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "INVALID_URL",
                "detail": "無効なURL",
                "method": "http"
            }
        
        integrity = None
        
        # Cookie完全性チェック
        cookie_path = self._latest_enter_cookie_path()
        if cookie_path:
            integrity = self._check_cookie_integrity(cookie_path)
            if not integrity["has_session"]:
                logger.warning(f"⚠️ Cookie missing _twitcasting_session: {cookie_path}")
        
        # キャッシュ回避
        bust = str(int(time.time()))
        probe_url = url + ("?t=" + bust if "?" not in url else "&t=" + bust)
        
        try:
            # HTTPリクエスト構築
            req = Request(probe_url)
            req.add_header("User-Agent", UA)
            req.add_header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
            req.add_header("Accept-Language", "ja,en-US;q=0.8,en;q=0.6")
            req.add_header("Cache-Control", "no-cache")
            req.add_header("Pragma", "no-cache")
            req.add_header("Referer", url)
            
            # Cookie付与
            if cookie_path:
                cookie_header = self._build_cookie_header_from_netscape(cookie_path)
                if cookie_header:
                    req.add_header("Cookie", cookie_header)
                    logger.debug(f"Cookie header set: {len(cookie_header)} chars")
            else:
                logger.warning("No cookie file found for authentication")
            
            with urlopen(req, timeout=self.timeout) as response:
                html = response.read(READ_SIZE).decode("utf-8", errors="ignore")
                
                # デバッグ用HTMLダンプ
                if self._debug_mode:
                    try:
                        DEBUG_HTML_PATH.write_text(html[:65536], encoding="utf-8")
                        logger.info(f"Debug HTML saved to {DEBUG_HTML_PATH}")
                    except Exception:
                        pass
                
                # AUTH_REQUIRED判定（最優先）
                auth_patterns = [
                    'tw-gate-required',
                    'membership-required',
                    'group-required',
                    '限定配信',
                    'ログインが必要',
                    'メンバー限定',
                    'フォロワー限定',
                    'グループ限定',
                    'membershipjoinplans',
                    'group_member_only',
                    'follower_only'
                ]
                
                auth_regex = '|'.join(re.escape(p) for p in auth_patterns)
                if re.search(auth_regex, html, re.IGNORECASE):
                    detail = "要ログイン（メン限/グル限の可能性）"
                    if cookie_path and integrity and not integrity["has_session"]:
                        detail += " - Cookie不完全(_twitcasting_session欠落)"
                    
                    return {
                        "is_live": False,
                        "movie_id": None,
                        "reason": "AUTH_REQUIRED",
                        "detail": detail,
                        "cookie_incomplete": cookie_path and integrity and not integrity["has_session"],
                        "method": "http"
                    }
                
                # 拡張LIVE判定パターン
                live_patterns = [
                    # 既存パターン
                    r'["\']is_live["\']\s*:\s*true',
                    r'"is_live"\s*:\s*true',
                    r"'is_live'\s*:\s*true",
                    r'data-is-live\s*=\s*["\']?true["\']?',
                    r'data-is-live="true"',
                    r"data-is-live='true'",
                    # 拡張パターン（表記ゆれ対応）
                    r'["\']isOnlive["\']\s*:\s*true',
                    r'["\']is_onlive["\']\s*:\s*(true|1)',
                    r'data-is-onlive\s*=\s*["\']?(true|1)["\']?',
                    r'isLive\s*:\s*true',
                    r'onLive\s*:\s*true',
                    # 間接的な兆候
                    r'tw-player-container',
                    r'<video[^>]*>',
                    r'class="tw-movie-thumbnail2"',
                ]
                
                is_live = False
                for pattern in live_patterns:
                    if re.search(pattern, html, re.IGNORECASE):
                        is_live = True
                        logger.debug(f"Live pattern matched: {pattern}")
                        break
                
                # JSON-LDチェック（追加の安全網）
                if not is_live:
                    for m in re.finditer(
                        r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>',
                        html, re.IGNORECASE | re.DOTALL
                    ):
                        try:
                            j = json.loads(m.group(1))
                            if isinstance(j, dict) and str(j.get("isLiveBroadcast", "")).lower() == "true":
                                is_live = True
                                logger.debug("Live detected via JSON-LD")
                                break
                        except Exception:
                            pass
                
                # movie_id抽出
                movie_id = self._extract_movie_id(html)
                
                if is_live:
                    logger.info(f"✅ LIVE detected (HTTP): {url} (movie_id={movie_id})")
                    return {
                        "is_live": True,
                        "movie_id": movie_id,
                        "reason": "LIVE",
                        "detail": "配信中",
                        "method": "http"
                    }
                
                # オフラインだがmovie_idがある場合は要注意
                if movie_id:
                    logger.warning(f"⚠️ movie_id={movie_id} found but NOT_LIVE (HTTP)")
                
                return {
                    "is_live": False,
                    "movie_id": movie_id,
                    "reason": "NOT_LIVE",
                    "detail": "配信していない",
                    "method": "http",
                    "needs_browser_check": bool(movie_id)  # movie_idがあれば要再検証
                }
        
        except HTTPError as e:
            if e.code in (401, 403):
                detail = f"HTTP {e.code} - 認証必要"
                if cookie_path and integrity and not integrity["has_session"]:
                    detail += " (_twitcasting_session欠落の可能性大)"
                
                return {
                    "is_live": False,
                    "movie_id": None,
                    "reason": "AUTH_REQUIRED",
                    "detail": detail,
                    "http_code": e.code,
                    "cookie_incomplete": cookie_path and integrity and not integrity["has_session"],
                    "method": "http"
                }
            else:
                return {
                    "is_live": False,
                    "movie_id": None,
                    "reason": f"HTTP_{e.code}",
                    "detail": f"HTTPエラー（{e.code}）",
                    "method": "http"
                }
        
        except URLError as e:
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "NETWORK_ERROR",
                "detail": f"通信エラー: {str(e)}",
                "method": "http"
            }
        
        except Exception as e:
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "UNKNOWN_ERROR",
                "detail": f"予期しないエラー: {str(e)}",
                "method": "http"
            }
    
    async def _get_chrome(self):
        """遅延初期化でChrome取得"""
        if not self._chrome:
            try:
                sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
                try:
                    from core.chrome_singleton import get_chrome_singleton
                except ImportError:
                    from chrome_singleton import get_chrome_singleton
                self._chrome = get_chrome_singleton()
                logger.info("Chrome singleton initialized for browser detection")
            except Exception as e:
                logger.error(f"Failed to get Chrome singleton: {e}")
                return None
        return self._chrome
    
    async def _check_status_browser(self, url: str) -> Dict:
        """
        ブラウザベースの配信状態チェック（JavaScript実行対応）
        永続コンテキスト使用で限定配信対応
        """
        chrome = await self._get_chrome()
        if not chrome:
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "BROWSER_UNAVAILABLE",
                "detail": "ブラウザ初期化失敗",
                "method": "browser"
            }
        
        ctx = None
        page = None
        
        try:
            # 永続+ログイン済みコンテキストで検知（録画と同じ条件）
            ctx = await chrome.ensure_headless(persistent=True)
            page = await ctx.new_page()
            
            logger.info(f"Browser check starting: {url}")
            
            # ページ読み込み
            await page.goto(url, wait_until="networkidle", timeout=15000)
            await page.wait_for_timeout(2000)  # JS実行待ち
            
            # JavaScriptで直接is_liveを取得
            is_live = await page.evaluate("""
                () => {
                    // 複数の場所から探す
                    if (typeof window.is_live !== 'undefined') return window.is_live;
                    if (window.App && window.App.is_live) return window.App.is_live;
                    if (window.TwitCasting && window.TwitCasting.is_live) return window.TwitCasting.is_live;
                    
                    // data属性から
                    const el = document.querySelector('[data-is-live]');
                    if (el) return el.dataset.isLive === 'true' || el.dataset.isLive === '1';
                    
                    // JSONから
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const text = script.textContent || '';
                        // is_live系
                        let match = text.match(/"is_live"\s*:\s*(true|false|1|0)/i);
                        if (match) return match[1] === 'true' || match[1] === '1';
                        // isOnlive系
                        match = text.match(/"isOnlive"\s*:\s*(true|false|1|0)/i);
                        if (match) return match[1] === 'true' || match[1] === '1';
                    }
                    
                    // videoタグの存在
                    const video = document.querySelector('video');
                    if (video && video.src) return true;
                    
                    return false;
                }
            """)
            
            # movie_id取得
            movie_id = await page.evaluate("""
                () => {
                    // data属性
                    let el = document.querySelector('[data-movie-id]');
                    if (el) return el.dataset.movieId;
                    
                    // JSONから
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const match = (script.textContent || '').match(/"movie_id"\s*:\s*(\d+)/);
                        if (match) return match[1];
                    }
                    
                    return null;
                }
            """)
            
            # AUTH_REQUIRED判定
            if not is_live:
                auth_required = await page.evaluate("""
                    () => {
                        const text = document.body.textContent.toLowerCase();
                        const hasAuthText = text.includes('ログインが必要') || 
                                           text.includes('メンバー限定') ||
                                           text.includes('グループ限定') ||
                                           text.includes('フォロワー限定');
                        const hasAuthElement = document.querySelector('.tw-gate-required') !== null ||
                                              document.querySelector('[class*="membership"]') !== null;
                        return hasAuthText || hasAuthElement;
                    }
                """)
                
                if auth_required:
                    return {
                        "is_live": False,
                        "movie_id": movie_id,
                        "reason": "AUTH_REQUIRED",
                        "detail": "要ログイン（ブラウザ検証）",
                        "method": "browser"
                    }
            
            if is_live:
                logger.info(f"✅ LIVE detected (Browser): {url} (movie_id={movie_id})")
                return {
                    "is_live": True,
                    "movie_id": movie_id,
                    "reason": "LIVE",
                    "detail": "配信中（ブラウザ検証）",
                    "method": "browser"
                }
            else:
                return {
                    "is_live": False,
                    "movie_id": movie_id,
                    "reason": "NOT_LIVE",
                    "detail": "配信していない（ブラウザ検証）",
                    "method": "browser"
                }
                
        except Exception as e:
            logger.error(f"Browser check error: {e}")
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "BROWSER_ERROR",
                "detail": f"ブラウザエラー: {str(e)}",
                "method": "browser"
            }
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
    
    async def _check_status_streamlink(self, url: str) -> Dict:
        """
        Streamlinkプローブ（最終手段）
        修正：Cookie/UA/Refererを正しくヘッダー指定
        """
        if not shutil.which("streamlink"):
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "STREAMLINK_UNAVAILABLE",
                "detail": "Streamlink未インストール",
                "method": "streamlink"
            }
        
        try:
            logger.info(f"Streamlink probe starting: {url}")
            
            # Cookieファイルパスを取得
            cookie_path = self._latest_enter_cookie_path()
            
            # Streamlinkコマンド構築
            cmd = ["streamlink", "--json", url, "best"]
            
            # 修正：Cookieをヘッダーとして正しく渡す
            if cookie_path and os.path.exists(cookie_path):
                cookie_header = self._build_cookie_header_from_netscape(cookie_path)
                if cookie_header:
                    cmd.extend(["--http-header", f"Cookie={cookie_header}"])
                    logger.debug(f"Streamlink cookie header injected ({len(cookie_header)} chars)")
                else:
                    logger.warning("Cookie file exists but no valid cookies extracted")
            
            # UA追加（TwitCastingは必須）
            cmd.extend(["--http-header", f"User-Agent={UA}"])
            
            # Referer追加（重要）
            cmd.extend(["--http-header", f"Referer={url}"])
            
            # タイムアウト設定
            cmd.extend(["--http-timeout", str(self.streamlink_timeout)])
            
            # デバッグモードならverbose
            if self._debug_mode:
                cmd.append("--verbose-player")
            
            logger.debug(f"Streamlink command: {' '.join(cmd[:3])}...")
            
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(), 
                timeout=self.streamlink_timeout + 2
            )
            
            stdout_str = stdout.decode("utf-8", errors="ignore")
            stderr_str = stderr.decode("utf-8", errors="ignore")
            
            # デバッグ出力
            if self._debug_mode and stderr_str:
                logger.debug(f"Streamlink stderr: {stderr_str[:500]}")
            
            if proc.returncode == 0:
                # JSON出力をパース
                try:
                    result_json = json.loads(stdout_str)
                    if "streams" in result_json and result_json["streams"]:
                        logger.info(f"✅ LIVE detected (Streamlink): {url}")
                        logger.debug(f"Available streams: {list(result_json['streams'].keys())}")
                        return {
                            "is_live": True,
                            "movie_id": None,
                            "reason": "LIVE",
                            "detail": "配信中（Streamlinkプローブ）",
                            "method": "streamlink",
                            "streams": list(result_json["streams"].keys())
                        }
                except json.JSONDecodeError:
                    # JSONパース失敗でも、streamsキーワードがあればLIVE判定
                    if '"streams"' in stdout_str:
                        logger.info(f"✅ LIVE detected (Streamlink, non-JSON): {url}")
                        return {
                            "is_live": True,
                            "movie_id": None,
                            "reason": "LIVE",
                            "detail": "配信中（Streamlinkプローブ）",
                            "method": "streamlink"
                        }
            
            # エラー詳細を解析
            error_detail = "配信していない"
            if "403" in stderr_str or "403" in stdout_str:
                error_detail = "認証エラー（403）"
            elif "404" in stderr_str or "404" in stdout_str:
                error_detail = "配信が見つからない（404）"
            elif "No playable streams" in stdout_str:
                error_detail = "再生可能なストリームなし"
            elif proc.returncode != 0:
                error_detail = f"Streamlink終了コード: {proc.returncode}"
            
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "NOT_LIVE",
                "detail": error_detail,
                "method": "streamlink"
            }
            
        except asyncio.TimeoutError:
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "STREAMLINK_TIMEOUT",
                "detail": f"Streamlinkタイムアウト（{self.streamlink_timeout}秒）",
                "method": "streamlink"
            }
        except Exception as e:
            logger.error(f"Streamlink probe error: {e}")
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "STREAMLINK_ERROR",
                "detail": f"Streamlinkエラー: {str(e)}",
                "method": "streamlink"
            }
    
    async def check_live(self, url: str) -> Dict:
        """
        3段階フォールバック配信チェック
        1. HTTP検知（高速）
        2. ブラウザ検知（movie_idがある場合）
        3. Streamlinkプローブ（最終手段）
        
        重要：最初に正規化して、全段階で同じURLを使用
        """
        # 最初に一度だけ正規化し、全段で同じURLを使う
        normalized_url = self._normalize_url(url)
        
        if not normalized_url:
            return {
                "is_live": False,
                "movie_id": None,
                "reason": "INVALID_URL",
                "detail": "無効なURL",
                "original_url": url
            }
        
        logger.info(f"Checking URL: {url} -> {normalized_url}")
        
        # Stage 1: HTTP検知（高速・軽量）
        result = await asyncio.get_running_loop().run_in_executor(
            None, self._check_status_http, normalized_url
        )
        
        # HTTPで確定した場合は即返却
        if result.get("is_live") or result.get("reason") == "AUTH_REQUIRED":
            return result
        
        # Stage 2: movie_idがあればブラウザ検知
        if result.get("needs_browser_check") or result.get("movie_id"):
            logger.info(f"⚠️ movie_id found, escalating to browser check")
            browser_result = await self._check_status_browser(normalized_url)
            
            # ブラウザで確定した場合
            if browser_result.get("is_live") or browser_result.get("reason") == "AUTH_REQUIRED":
                return browser_result
            
            # まだmovie_idがある場合は要注意
            if browser_result.get("movie_id"):
                logger.warning(f"⚠️ movie_id={browser_result['movie_id']} still NOT_LIVE after browser check")
                
                # Stage 3: Streamlinkプローブ（最終手段）
                logger.info("Escalating to Streamlink probe")
                streamlink_result = await self._check_status_streamlink(normalized_url)
                
                if streamlink_result.get("is_live"):
                    # movie_idを引き継ぐ
                    streamlink_result["movie_id"] = browser_result.get("movie_id")
                    return streamlink_result
        
        # 全段階でNOT_LIVE
        return result
    
    # 互換性のための別名
    async def check_status(self, url: str) -> Dict:
        """互換インターフェース"""
        return await self.check_live(url)
    
    async def check_live_status(self, url: str) -> Dict:
        """互換インターフェース"""
        return await self.check_live(url)
    
    async def check_with_cookie_repair(self, url: str) -> Dict:
        """Cookie自動修復機能付きチェック（互換性維持）"""
        result = await self.check_live(url)
        
        # AUTH_REQUIRED かつ Cookie不完全 かつ 未修復なら
        if (result.get("reason") == "AUTH_REQUIRED" and 
            result.get("cookie_incomplete") and 
            not self._cookie_repair_attempted):
            
            logger.warning("🔧 Cookie repair needed - attempting re-login...")
            self._cookie_repair_attempted = True
            
            try:
                sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
                try:
                    from recorder_wrapper import RecorderWrapper
                except ImportError:
                    from auto.recorder_wrapper import RecorderWrapper
                
                logger.info("Forcing re-login to obtain _twitcasting_session...")
                success = await RecorderWrapper.ensure_login(force=True)
                
                if success:
                    logger.info("✅ Re-login successful, waiting for cookie propagation...")
                    await asyncio.sleep(1.5)
                    
                    # 再チェック
                    logger.info("Re-checking after cookie repair...")
                    result = await self.check_live(url)
                    logger.info(f"Re-check result: reason={result.get('reason')}, "
                               f"is_live={result.get('is_live')}")
                else:
                    logger.error("❌ Re-login failed")
                    
            except Exception as e:
                logger.error(f"Cookie repair failed: {e}")
        
        return result


# テスト用
if __name__ == "__main__":
    import asyncio
    
    async def test():
        if len(sys.argv) < 2:
            print("Usage: python live_detector.py <URL>")
            print("\nOptions:")
            print("  DEBUG_LIVE_DETECTOR=true  - Enable debug output")
            print("\nExamples:")
            print("  python live_detector.py https://twitcasting.tv/username")
            print("  python live_detector.py c:username")
            print("  python live_detector.py g:117191...")
            sys.exit(1)
        
        detector = LiveDetector()
        url = sys.argv[1]
        
        print(f"Original URL: {url}")
        print(f"Normalized URL: {detector._normalize_url(url)}")
        print(f"Read size: {READ_SIZE} bytes")
        print(f"Debug mode: {detector._debug_mode}")
        
        # Cookie状態確認
        cookie_path = detector._latest_enter_cookie_path()
        if cookie_path:
            integrity = detector._check_cookie_integrity(cookie_path)
            print(f"\nCookie file: {os.path.basename(cookie_path)}")
            print(f"  tc_id: {'✅' if integrity['has_tc_id'] else '❌'}")
            print(f"  tc_ss: {'✅' if integrity['has_tc_ss'] else '❌'}")
            print(f"  _twitcasting_session: {'✅' if integrity['has_session'] else '❌'}")
            print(f"  Complete: {'✅' if integrity['is_complete'] else '❌'}")
            
            # Cookieヘッダー構築テスト
            cookie_header = detector._build_cookie_header_from_netscape(cookie_path)
            if cookie_header:
                print(f"  Cookie header: {len(cookie_header)} chars")
                if detector._debug_mode:
                    print(f"  First 100 chars: {cookie_header[:100]}...")
        else:
            print("\n⚠️ No cookie file found")
        
        print("\n=== Starting 3-stage detection ===")
        result = await detector.check_live(url)
        
        print("\n=== Result ===")
        print(json.dumps(result, indent=2, ensure_ascii=False))
        
        # 判定理由の説明
        if result.get("reason") == "AUTH_REQUIRED":
            print("\n⚠️  認証が必要な配信です")
            if result.get("cookie_incomplete"):
                print("    _twitcasting_sessionが不足しています")
            print("    メンバー限定またはグループ限定の可能性があります")
        elif result.get("is_live"):
            print(f"\n✅ 配信中です（検出方法: {result.get('method', 'unknown')}）")
            if result.get("movie_id"):
                print(f"    Movie ID: {result['movie_id']}")
            if result.get("streams"):
                print(f"    Available streams: {', '.join(result['streams'])}")
        else:
            print(f"\n❌ 配信していません（検証方法: {result.get('method', 'unknown')}）")
            if result.get("movie_id"):
                print(f"    Movie ID: {result['movie_id']} (要注意)")
        
        if detector._debug_mode and DEBUG_HTML_PATH.exists():
            print(f"\n📄 Debug HTML saved to: {DEBUG_HTML_PATH}")
    
    asyncio.run(test())