import base64
import json
from typing import Any, Dict, Optional

import requests

API_URL = "https://crypto.happ.su/api-v2.php"


def _normalize_version(version: str) -> str:
    v = (version or "v4").strip().lower()
    if v not in {"v4", "v5", "4", "5"}:
        raise ValueError("version must be 'v4' or 'v5'")
    return "v" + v[-1]


def _extract_string(data: Any) -> Optional[str]:
    if isinstance(data, str):
        return data
    if isinstance(data, dict):
        for key in (
            "link",
            "url",
            "happLink",
            "result",
            "encrypted",
            "data",
            "value",
            "crypt",
        ):
            value = data.get(key)
            if isinstance(value, str) and value.startswith("happ://crypt"):
                return value
        for value in data.values():
            if isinstance(value, str) and value.startswith("happ://crypt"):
                return value
    return None


def create_happ_crypto_link(content: str, version: str = "v4", as_link: bool = True, timeout: int = 20) -> str:
    v = _normalize_version(version)
    payloads = [
        {"content": content, "version": v, "asLink": as_link},
        {"content": content, "version": v, "link": as_link},
        {"url": content, "version": v, "asLink": as_link},
        {"text": content, "version": v, "asLink": as_link},
        {"content": content, "type": v, "asLink": as_link},
    ]

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "happ-python/1.0",
    }

    last_error = None
    for payload in payloads:
        try:
            resp = requests.post(API_URL, headers=headers, data=json.dumps(payload), timeout=timeout)
            text = resp.text.strip()
            if resp.ok:
                try:
                    parsed = resp.json()
                except Exception:
                    parsed = text
                link = _extract_string(parsed)
                if link:
                    return link
                if isinstance(parsed, str) and parsed.startswith("happ://crypt"):
                    return parsed
                if text.startswith("happ://crypt"):
                    return text
            last_error = f"HTTP {resp.status_code}: {text[:300]}"
        except Exception as e:
            last_error = str(e)

    raise RuntimeError(f"Failed to generate happ crypto link via HAPP API: {last_error}")


def createHappCryptoLink(content: str, version: str = "v4", as_link: bool = True) -> str:
    return create_happ_crypto_link(content, version=version, as_link=as_link)
