#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Live Detector for TwitCasting (修正版)
配信状態を軽量チェック（Chrome不要・外部ライブラリ不要）
重大バグ修正版
- 401/403正しく分類
- ゲートページ誤検知防止
- g:/ig:形式対応
"""
import asyncio
import json
import re
import time
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import urllib.request
import urllib.error

# エラーコード定義
ERROR_CODES = {
    "SUCCESS": "成功",
    "NOT_LIVE": "配信してない",
    "AUTH_REQUIRED": "認証必要（メン限/グル限）",
    "NETWORK_ERROR": "ネットワークエラー",
    "PARSE_ERROR": "HTMLパースエラー",
    "TIMEOUT": "タイムアウト"
}

class LiveDetector:
    """配信状態検知（標準ライブラリのみ使用）"""
    
    def __init__(self, auth_enabled: bool = False):
        self.auth_enabled = auth_enabled
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, *args):
        pass
            
    async def check_live_status(self, url: str) -> Dict:
        """
        配信状態をチェック（HTTPリクエストのみ）
        
        Returns:
            {
                "is_live": bool,
                "title": str,
                "viewers": int,
                "detected_at": str,
                "method": "html|auth|error",
                "error_code": str
            }
        """
        # URL正規化
        user_id = self._extract_user_id(url)
        if not user_id:
            return self._error_result("PARSE_ERROR", "Invalid URL")
            
        # c:/g:/ig:を含めて正規化
        if ":" in user_id:
            normalized_url = f"https://twitcasting.tv/{user_id}"
        else:
            normalized_url = f"https://twitcasting.tv/{user_id}"
        
        # 軽量HTMLチェック
        try:
            # 非同期でurllib実行
            result = await asyncio.get_event_loop().run_in_executor(
                None, self._check_html_sync, normalized_url
            )
            return result
            
        except asyncio.TimeoutError:
            return self._error_result("TIMEOUT", "Request timeout")
        except Exception as e:
            return self._error_result("NETWORK_ERROR", str(e))
            
    def _check_html_sync(self, url: str) -> Dict:
        """同期的なHTMLチェック（urllib使用）"""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=10) as response:
                # ステータスコード確認
                if response.status in (401, 403):
                    return self._error_result("AUTH_REQUIRED", f"HTTP {response.status}")
                if response.status != 200:
                    return self._error_result("NETWORK_ERROR", f"HTTP {response.status}")
                    
                # HTMLを読み込み
                html = response.read().decode('utf-8', errors='ignore')
                
        except urllib.error.HTTPError as e:
            # 401/403を正しく分類
            if e.code in (401, 403):
                return self._error_result("AUTH_REQUIRED", f"HTTP {e.code}")
            return self._error_result("NETWORK_ERROR", f"HTTP {e.code}")
        except urllib.error.URLError as e:
            return self._error_result("NETWORK_ERROR", str(e))
        except Exception as e:
            return self._error_result("NETWORK_ERROR", str(e))
            
        # 先にゲート検出（誤検知防止）
        is_restricted = any([
            'tw-gate-required' in html,
            'membership-required' in html,
            'group-required' in html,
            '限定配信' in html,
            'tw-membership-gate' in html,
            'tw-group-gate' in html
        ])
        
        # 認証モードが無効なら、ゲート検出時点でAUTH_REQUIRED
        if is_restricted and not self.auth_enabled:
            return self._error_result("AUTH_REQUIRED", "Restricted gate detected")
            
        # その後にLIVE判定
        is_live = any([
            '"is_live":true' in html,
            'data-is-live="true"' in html,
            'tw-player-container-live' in html,
            'LIVE</span>' in html,
            'js-live-indicator' in html
        ])
        
        # タイトル抽出
        title = "Unknown"
        title_match = re.search(r'<title>([^<]+)</title>', html)
        if title_match:
            title = title_match.group(1).strip()
            
        # 視聴者数
        viewers = 0
        viewers_match = re.search(r'"viewer_count":(\d+)', html)
        if viewers_match:
            viewers = int(viewers_match.group(1))
            
        return {
            "is_live": is_live,
            "title": title,
            "viewers": viewers,
            "detected_at": datetime.now().isoformat(),
            "method": "html",
            "error_code": "SUCCESS" if is_live else "NOT_LIVE"
        }
            
    def _extract_user_id(self, url: str) -> Optional[str]:
        """URL からユーザーID抽出（g:/ig:対応）"""
        u = str(url).strip()
        
        # フルURL
        m = re.search(r'^https?://(?:www\.)?twitcasting\.tv/([^/\?]+)', u, re.IGNORECASE)
        if m:
            return m.group(1)
            
        # c:/g:/ig:形式（接頭辞を残す）
        m = re.search(r'^(c|g|ig):([^/\?]+)$', u, re.IGNORECASE)
        if m:
            return f"{m.group(1).lower()}:{m.group(2)}"
            
        # 素のID
        m = re.search(r'^([^/\?:]+)$', u)
        if m:
            return m.group(1)
            
        return None
        
    def _error_result(self, code: str, detail: str = "") -> Dict:
        """エラー結果生成"""
        return {
            "is_live": False,
            "title": "",
            "viewers": 0,
            "detected_at": datetime.now().isoformat(),
            "method": "error",
            "error_code": code,
            "error_detail": detail
        }

# テスト用
async def test():
    print("=== Live Detector Test ===")
    async with LiveDetector() as detector:
        test_urls = [
            "https://twitcasting.tv/twitcasting_jp",
            "c:twitcasting_jp",
            "g:group_test",
            "ig:item_test",
            "invalid_url"
        ]
        
        for url in test_urls:
            result = await detector.check_live_status(url)
            print(f"{url}: {json.dumps(result, ensure_ascii=False, indent=2)}")

if __name__ == "__main__":
    asyncio.run(test())