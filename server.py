import base64
import hashlib
import json
import os
import secrets
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import qrcode
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from user_agents import parse as parse_ua

from happ_crypto import create_happ_crypto_link

BASE_DIR = Path(__file__).resolve().parent
PUBLIC_DIR = BASE_DIR / "public"
DB_PATH = BASE_DIR / "db.json"

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "changeme123")
APPSTORE_URL = "https://apps.apple.com/ru/app/happ-proxy-utility-plus/id6746188973"
PLAYSTORE_URL = "https://play.google.com/store/apps/details?id=com.happproxy&hl=ru"
WINDOWS_DOWNLOAD_URL = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/setup-Happ.x64.exe"
MACOS_DOWNLOAD_URL = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/Happ-x64.dmg"
LINUX_DOWNLOAD_URL = "https://github.com/Happ-proxy/happ-desktop/releases/latest/download/happ-desktop-x86_64.AppImage"

app = FastAPI()
security = HTTPBasic()

link_locks: Dict[str, threading.Lock] = {}
group_locks: Dict[str, threading.Lock] = {}
request_cache: Dict[str, float] = {}
global_lock = threading.Lock()


class GenerateRequest(BaseModel):
    subscriptionUrl: str
    maxActivations: int = 1


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_id(size: int = 16) -> str:
    return secrets.token_hex(size)


def load_db() -> Dict[str, Any]:
    if not DB_PATH.exists():
        return {"links": []}
    try:
        return json.loads(DB_PATH.read_text("utf-8"))
    except Exception:
        return {"links": []}


def save_db(data: Dict[str, Any]) -> None:
    DB_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")


def normalize_input(value: str = "") -> str:
    v = str(value or "").strip().replace("\u200B", "").strip()
    if not v:
        return ""
    if v.startswith("[") and "](" in v and v.endswith(")"):
        try:
            v = v[v.index("](") + 2 : -1].strip()
        except Exception:
            pass
    if v.startswith("<") and v.endswith(">"):
        v = v[1:-1].strip()
    v = v.strip("\"'").strip()
    if not v.lower().startswith(("http://", "https://")) and "." in v:
        v = "https://" + v
    return v


def is_valid_http_url(value: str) -> bool:
    try:
        u = urlparse(value)
        return u.scheme in {"http", "https"} and bool(u.netloc)
    except Exception:
        return False


def extract_username_from_subscription_url(value: str) -> str:
    try:
        parts = [p for p in urlparse(value).path.split("/") if p]
        return parts[-1] if parts else ""
    except Exception:
        return ""


def get_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else ""


def normalize_ip(ip: str) -> str:
    ip = str(ip or "").strip()
    return ip[7:] if ip.startswith("::ffff:") else ip


def detect_windows_version(ua: str, os_name: str, os_version: str, browser_name: str, browser_version: str) -> tuple[str, str]:
    ua_lower = ua.lower()
    if os_name != "Windows":
        return os_name, os_version
    if os_version == "10":
        try:
            bv = int((browser_version or "0").split(".")[0])
        except Exception:
            bv = 0
        is_modern = (
            (browser_name == "Chrome" and bv >= 100)
            or (browser_name == "Edge" and bv >= 90)
            or (browser_name == "Firefox" and bv >= 100)
        )
        is_64bit = "win64" in ua_lower or "x64" in ua_lower
        is_not_old = "windows 6." not in ua_lower and "windows nt 6." not in ua_lower
        if is_modern and is_64bit and is_not_old:
            return "Windows", "11"
    return os_name, os_version


async def parse_client(request: Request) -> Dict[str, Any]:
    ua = request.headers.get("user-agent", "")
    body = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    parsed = parse_ua(ua)
    browser_name = parsed.browser.family or ""
    browser_version = parsed.browser.version_string or ""
    os_name = parsed.os.family or ""
    os_version = parsed.os.version_string or ""
    os_name, os_version = detect_windows_version(ua, os_name, os_version, browser_name, browser_version)
    device_type = "desktop"
    if parsed.is_tablet:
        device_type = "tablet"
    elif parsed.is_mobile:
        device_type = "mobile"
    return {
        "ip": get_client_ip(request),
        "userAgent": ua,
        "browser": " ".join([x for x in [browser_name, browser_version] if x]).strip(),
        "browserName": browser_name,
        "browserVersion": browser_version,
        "os": body.get("os") or ((f"{os_name} {os_version}").strip() if os_name else ""),
        "osName": os_name,
        "osVersion": os_version,
        "deviceType": body.get("deviceType") or device_type,
        "deviceVendor": body.get("deviceVendor", ""),
        "deviceModel": body.get("deviceModel", ""),
        "platform": body.get("platform", ""),
        "language": body.get("language", ""),
        "languages": ",".join(body.get("languages", [])) if isinstance(body.get("languages"), list) else "",
        "screen": body.get("screen", ""),
        "timezone": body.get("timezone", ""),
        "clientId": body.get("clientId", ""),
        "pageSessionId": body.get("pageSessionId", ""),
        "hardwareConcurrency": body.get("hardwareConcurrency", ""),
        "deviceMemory": body.get("deviceMemory", ""),
        "colorDepth": body.get("colorDepth", ""),
        "pixelRatio": body.get("pixelRatio", ""),
        "touchPoints": body.get("touchPoints", ""),
        "viewport": body.get("viewport", ""),
        "referrer": body.get("referrer", ""),
        "pageUrl": body.get("pageUrl", ""),
        "pagePath": body.get("pagePath", ""),
    }


def make_device_key(u: Dict[str, Any]) -> str:
    client_id = str(u.get("clientId", ""))[-12:]
    os_name = u.get("osName") or "unknown"
    os_version = f" {u.get('osVersion')}" if u.get("osVersion") else ""
    browser_name = u.get("browserName") or "unknown"
    browser_version = f" {u.get('browserVersion')}" if u.get("browserVersion") else ""
    screen = u.get("screen") or "unknown"
    timezone_name = u.get("timezone") or "unknown"
    if os_name == "Windows":
        return f"windows|{client_id}|{os_version.strip()}|{browser_name}{browser_version}|{screen}|{timezone_name}"
    if os_name in {"iOS", "Mac OS X", "macOS"}:
        return f"apple|{client_id}|{os_name}{os_version}|{browser_name}{browser_version}|{screen}|{timezone_name}"
    if os_name == "Android":
        return f"android|{client_id}|{os_version.strip()}|{browser_name}{browser_version}|{screen}|{timezone_name}"
    return f"{os_name}{os_version}|{client_id}|{browser_name}{browser_version}|{screen}|{timezone_name}"


def make_raw_device_key(u: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(u.get("clientId", "")),
            normalize_ip(u.get("ip", "")),
            str(u.get("browser", "")),
            str(u.get("browserVersion", "")),
            str(u.get("os", "")),
            str(u.get("osVersion", "")),
            str(u.get("platform", "")),
            str(u.get("language", "")),
            str(u.get("screen", "")),
            str(u.get("timezone", "")),
            str(u.get("deviceType", "")),
            str(u.get("deviceVendor", "")),
            str(u.get("deviceModel", "")),
            str(u.get("hardwareConcurrency", "")),
            str(u.get("deviceMemory", "")),
            str(u.get("colorDepth", "")),
            str(u.get("pixelRatio", "")),
            str(u.get("touchPoints", "")),
            str(u.get("viewport", "")),
        ]
    )


def get_group_links(db: Dict[str, Any], subscription_url: str) -> List[Dict[str, Any]]:
    return [x for x in db.get("links", []) if x.get("subscriptionUrl") == subscription_url]


def get_group_primary_usage(db: Dict[str, Any], subscription_url: str) -> Optional[Dict[str, Any]]:
    for link in get_group_links(db, subscription_url):
        if link.get("activations"):
            return link["activations"][0]
    return None


def get_group_primary_device_key(db: Dict[str, Any], subscription_url: str) -> Optional[str]:
    usage = get_group_primary_usage(db, subscription_url)
    return usage.get("deviceKey") if usage else None


def has_group_activations(db: Dict[str, Any], subscription_url: str) -> bool:
    return any(bool(link.get("activations")) for link in get_group_links(db, subscription_url))


def beautify_platform(u: Dict[str, Any]) -> str:
    p = u.get("osName", "")
    if p == "iOS":
        return "iPhone / iPad"
    if p == "Android":
        return "Android"
    if p == "Windows":
        return "Windows"
    if p in {"Mac OS X", "macOS"}:
        return "macOS"
    if p == "Linux":
        return "Linux"
    return "Другое устройство"


def beautify_device_type(u: Dict[str, Any]) -> str:
    t = str(u.get("deviceType", "")).lower()
    if t == "tablet":
        return "Планшет"
    if t == "mobile":
        return "Телефон"
    platform = u.get("osName", "")
    if platform in {"Android", "iOS"}:
        return "Мобильное устройство"
    return "Компьютер"


def build_user_facing_device_info(usage: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not usage:
        return None
    return {
        "title": f"{beautify_platform(usage)} • {beautify_device_type(usage)}",
        "platform": beautify_platform(usage),
        "deviceType": beautify_device_type(usage),
        "os": usage.get("os") or "Неизвестная ОС",
        "browser": usage.get("browser") or "Неизвестный браузер",
        "model": " ".join([x for x in [usage.get("deviceVendor", ""), usage.get("deviceModel", "")] if x]).strip(),
        "screen": usage.get("screen", ""),
        "timezone": usage.get("timezone", ""),
        "language": usage.get("language", ""),
        "firstSeenAt": usage.get("at", ""),
        "lastSeenAt": usage.get("at", ""),
        "deviceKey": usage.get("deviceKey"),
        "ip": normalize_ip(usage.get("ip", "")),
    }


def is_duplicate_request(token: str, client_id: str, page_session_id: str) -> bool:
    key = f"{token}:{client_id}:{page_session_id}"
    import time
    now = time.time()
    old = request_cache.get(key)
    if old and now - old < 5:
        return True
    request_cache[key] = now
    for k, v in list(request_cache.items()):
        if now - v > 30:
            request_cache.pop(k, None)
    return False


def map_link_for_admin(item: Dict[str, Any]) -> Dict[str, Any]:
    activations = item.get("activations", [])
    violations = item.get("violations", [])
    unique_devices = len(set([x.get("deviceKey") for x in activations + violations if x.get("deviceKey")]))
    total_used = len(activations) + len(violations)
    return {
        "id": item.get("id"),
        "token": item.get("token"),
        "username": item.get("username"),
        "subscriptionUrl": item.get("subscriptionUrl"),
        "happLink": item.get("happLink"),
        "maxActivations": item.get("maxActivations"),
        "usedCount": total_used,
        "remaining": max(int(item.get("maxActivations", 0)) - total_used, 0),
        "status": item.get("status"),
        "createdAt": item.get("createdAt"),
        "lastUsedAt": item.get("lastUsedAt"),
        "primaryDeviceKey": activations[0].get("deviceKey") if activations else None,
        "uniqueDevices": unique_devices,
        "sameDeviceCount": len(activations),
        "foreignDeviceCount": len(violations),
        "isViolator": len(violations) > 0 or item.get("status") == "violator",
        "activations": activations,
        "violations": violations,
    }


def build_groups(links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    groups: Dict[str, Dict[str, Any]] = {}
    for item in links:
        key = item.get("subscriptionUrl") or "__empty__"
        if key not in groups:
            groups[key] = {
                "subscriptionUrl": item.get("subscriptionUrl"),
                "username": item.get("username"),
                "links": [],
                "maxActivationsTotal": 0,
                "usedCountTotal": 0,
                "allActivations": [],
                "allViolations": [],
                "violatorLinks": 0,
                "lastUsedAt": None,
            }
        g = groups[key]
        g["links"].append(item)
        g["maxActivationsTotal"] += int(item.get("maxActivations", 0))
        activations = item.get("activations", [])
        violations = item.get("violations", [])
        g["usedCountTotal"] += len(activations) + len(violations)
        g["allActivations"].extend(activations)
        g["allViolations"].extend(violations)
        if violations:
            g["violatorLinks"] += 1
        if not g["lastUsedAt"] or (item.get("lastUsedAt") and item.get("lastUsedAt") > g["lastUsedAt"]):
            g["lastUsedAt"] = item.get("lastUsedAt") or g["lastUsedAt"]
    result = []
    for g in groups.values():
        unique_devices = len(set([x.get("deviceKey") for x in g["allActivations"] + g["allViolations"] if x.get("deviceKey")]))
        primary_usage = g["allActivations"][0] if g["allActivations"] else None
        result.append(
            {
                "subscriptionUrl": g["subscriptionUrl"],
                "username": g["username"],
                "linksCount": len(g["links"]),
                "linkIds": [x.get("id") for x in g["links"]],
                "tokens": [x.get("token") for x in g["links"]],
                "maxActivationsTotal": g["maxActivationsTotal"],
                "usedCountTotal": g["usedCountTotal"],
                "remainingTotal": max(g["maxActivationsTotal"] - g["usedCountTotal"], 0),
                "uniqueDevices": unique_devices,
                "violatorLinks": g["violatorLinks"],
                "foreignDeviceCountTotal": len(g["allViolations"]),
                "primaryDeviceKey": primary_usage.get("deviceKey") if primary_usage else None,
                "boundDevice": build_user_facing_device_info(primary_usage),
                "isViolator": len(g["allViolations"]) > 0 or g["violatorLinks"] > 0,
                "lastUsedAt": g["lastUsedAt"],
                "links": [map_link_for_admin(x) for x in g["links"]],
            }
        )
    return result


def get_lock(bucket: Dict[str, threading.Lock], key: str) -> threading.Lock:
    with global_lock:
        if key not in bucket:
            bucket[key] = threading.Lock()
        return bucket[key]


def require_admin(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    ok_user = secrets.compare_digest(credentials.username, ADMIN_USER)
    ok_pass = secrets.compare_digest(credentials.password, ADMIN_PASS)
    if not (ok_user and ok_pass):
        raise HTTPException(status_code=401, detail="Invalid credentials", headers={"WWW-Authenticate": "Basic"})


# ==================== ОСНОВНЫЕ МАРШРУТЫ (без префикса) ====================

@app.get("/")
async def root():
    return FileResponse(PUBLIC_DIR / "index.html")


@app.get("/admin.html")
async def admin_html(_: None = Depends(require_admin)):
    return FileResponse(PUBLIC_DIR / "admin.html")


@app.post("/api/generate")
async def api_generate(payload: GenerateRequest):
    db = load_db()
    subscription_url = normalize_input(payload.subscriptionUrl)
    max_activations = int(payload.maxActivations or 1)
    if not subscription_url or not is_valid_http_url(subscription_url):
        return JSONResponse({"ok": False, "error": "URL подписки не валиден"}, status_code=400)
    username = extract_username_from_subscription_url(subscription_url)
    if not username:
        return JSONResponse({"ok": False, "error": "Не удалось извлечь имя пользователя из ссылки"}, status_code=400)
    if max_activations < 1 or max_activations > 100:
        return JSONResponse({"ok": False, "error": "Лимит активаций должен быть от 1 до 100"}, status_code=400)
    try:
        happ_link = create_happ_crypto_link(subscription_url, "v4", True)
        if not happ_link.startswith("happ://crypt"):
            raise RuntimeError("Не удалось сгенерировать корректную happ-ссылку")
        token = make_id(16)
        db.setdefault("links", []).append(
            {
                "id": make_id(10),
                "token": token,
                "username": username,
                "subscriptionUrl": subscription_url,
                "happLink": happ_link,
                "maxActivations": max_activations,
                "usedCount": 0,
                "status": "active",
                "createdAt": now_iso(),
                "lastUsedAt": None,
                "activations": [],
                "violations": [],
            }
        )
        save_db(db)
        return {"ok": True, "onceLink": f"/r/{token}", "username": username, "maxActivations": max_activations, "happLink": happ_link}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e) or "Ошибка генерации"}, status_code=500)


@app.get("/api/link/{token}")
async def api_link(token: str):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("token") == token), None)
    if not item:
        return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
    activations = item.get("activations", [])
    violations = item.get("violations", [])
    first = activations[0] if activations else None
    total_used = len(activations) + len(violations)
    remaining = max(int(item.get("maxActivations", 0)) - total_used, 0)
    return {
        "ok": True,
        "username": item.get("username"),
        "usedCount": total_used,
        "maxActivations": item.get("maxActivations"),
        "remaining": remaining,
        "status": item.get("status"),
        "appStoreUrl": APPSTORE_URL,
        "playStoreUrl": PLAYSTORE_URL,
        "windowsDownloadUrl": WINDOWS_DOWNLOAD_URL,
        "macosDownloadUrl": MACOS_DOWNLOAD_URL,
        "linuxDownloadUrl": LINUX_DOWNLOAD_URL,
        "happLink": item.get("happLink"),
        "boundDevice": {
            "os": first.get("os"),
            "browser": first.get("browser"),
            "deviceType": first.get("deviceType"),
            "screen": first.get("screen"),
            "timezone": first.get("timezone"),
            "language": first.get("language"),
            "firstSeenAt": first.get("at"),
            "ip": first.get("ip"),
        } if first else None,
    }


@app.post("/api/check-device/{token}")
async def api_check_device(token: str, request: Request):
    with get_lock(link_locks, token):
        db = load_db()
        item = next((x for x in db.get("links", []) if x.get("token") == token), None)
        if not item:
            return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
        client = await parse_client(request)
        device_key = make_device_key(client)
        group_has = has_group_activations(db, item.get("subscriptionUrl"))
        group_primary_key = get_group_primary_device_key(db, item.get("subscriptionUrl"))
        activations = item.get("activations", [])
        violations = item.get("violations", [])
        total_used = len(activations) + len(violations)
        remaining = max(int(item.get("maxActivations", 0)) - total_used, 0)
        status = "ok"
        message = None
        bound_device = None
        if group_has:
            if group_primary_key == device_key:
                status = "same-device"
                message = "Это устройство уже активировало подписку"
            else:
                status = "different-device"
                message = "Эта подписка уже активирована на другом устройстве"
                bound_device = build_user_facing_device_info(get_group_primary_usage(db, item.get("subscriptionUrl")))
        return {
            "ok": True,
            "status": status,
            "message": message,
            "boundDevice": bound_device,
            "deviceKey": device_key,
            "remaining": remaining,
            "happLink": item.get("happLink"),
            "appStoreUrl": APPSTORE_URL,
            "playStoreUrl": PLAYSTORE_URL,
            "windowsDownloadUrl": WINDOWS_DOWNLOAD_URL,
            "macosDownloadUrl": MACOS_DOWNLOAD_URL,
            "linuxDownloadUrl": LINUX_DOWNLOAD_URL,
        }


@app.post("/api/redeem-preview/{token}")
async def api_redeem_preview(token: str, request: Request):
    with get_lock(link_locks, token):
        db = load_db()
        item = next((x for x in db.get("links", []) if x.get("token") == token), None)
        if not item:
            return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
        activations = item.get("activations", [])
        violations = item.get("violations", [])
        total_used = len(activations) + len(violations)
        remaining = max(int(item.get("maxActivations", 0)) - total_used, 0)
        if total_used >= int(item.get("maxActivations", 0)):
            item["status"] = "used"
            save_db(db)
            return JSONResponse({"ok": False, "error": "Лимит активаций исчерпан"}, status_code=410)
        if not str(item.get("happLink", "")).startswith("happ://crypt"):
            return JSONResponse({"ok": False, "error": "Повреждённая happ-ссылка"}, status_code=500)
        await parse_client(request)
        first = activations[0] if activations else None
        return {
            "ok": True,
            "happLink": item.get("happLink"),
            "remaining": remaining,
            "appStoreUrl": APPSTORE_URL,
            "playStoreUrl": PLAYSTORE_URL,
            "windowsDownloadUrl": WINDOWS_DOWNLOAD_URL,
            "macosDownloadUrl": MACOS_DOWNLOAD_URL,
            "linuxDownloadUrl": LINUX_DOWNLOAD_URL,
            "boundDevice": {
                "os": first.get("os"),
                "browser": first.get("browser"),
                "deviceType": first.get("deviceType"),
                "screen": first.get("screen"),
                "timezone": first.get("timezone"),
                "language": first.get("language"),
                "firstSeenAt": first.get("at"),
                "ip": first.get("ip"),
            } if first else None,
        }


@app.post("/api/redeem-confirm/{token}")
async def api_redeem_confirm(token: str, request: Request):
    db = load_db()
    initial_item = next((x for x in db.get("links", []) if x.get("token") == token), None)
    if not initial_item:
        return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
    with get_lock(group_locks, initial_item.get("subscriptionUrl")):
        db = load_db()
        item = next((x for x in db.get("links", []) if x.get("token") == token), None)
        if not item:
            return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
        client = await parse_client(request)
        device_key = make_device_key(client)
        raw_device_key = make_raw_device_key(client)
        now = now_iso()
        if is_duplicate_request(token, client.get("clientId", ""), client.get("pageSessionId", "")):
            return {"ok": True, "duplicate": True}
        item.setdefault("activations", [])
        item.setdefault("violations", [])
        group_links = [x for x in db.get("links", []) if x.get("subscriptionUrl") == item.get("subscriptionUrl")]
        all_activations = []
        all_violations = []
        for link in group_links:
            all_activations.extend(link.get("activations", []))
            all_violations.extend(link.get("violations", []))
        all_activations.sort(key=lambda x: x.get("at", ""))
        group_has = len(all_activations) > 0
        group_primary_key = all_activations[0].get("deviceKey") if all_activations else None
        total_used = len(all_activations) + len(all_violations)
        total_group_limit = sum(int(link.get("maxActivations", 0)) for link in group_links)
        if total_used >= total_group_limit:
            for link in group_links:
                link["status"] = "used"
            save_db(db)
            return JSONResponse({"ok": False, "error": "Лимит активаций группы исчерпан"}, status_code=410)
        payload = {"at": now, **client, "deviceKey": device_key, "rawDeviceKey": raw_device_key}
        if group_has:
            if device_key == group_primary_key:
                item["activations"].append(payload)
                item["lastUsedAt"] = now
                new_total_used = total_used + 1
                item["status"] = "used" if new_total_used >= total_group_limit else ("violator" if item.get("violations") else "active")
                save_db(db)
                return {"ok": True, "remaining": max(total_group_limit - new_total_used, 0)}
            else:
                item["violations"].append({**payload, "reason": "different-device"})
                item["status"] = "violator"
                item["lastUsedAt"] = now
                save_db(db)
                remaining = max(total_group_limit - (total_used + 1), 0)
                return JSONResponse(
                    {
                        "ok": False,
                        "error": "Эта подписка уже активирована на другом устройстве (активация списана)",
                        "boundDevice": build_user_facing_device_info(all_activations[0]),
                        "remaining": remaining,
                    },
                    status_code=403,
                )
        item["activations"].append(payload)
        item["lastUsedAt"] = now
        new_total_used = total_used + 1
        item["status"] = "used" if new_total_used >= total_group_limit else ("violator" if item.get("violations") else "active")
        save_db(db)
        return {"ok": True, "remaining": max(total_group_limit - new_total_used, 0)}


@app.get("/api/admin/links")
async def api_admin_links(_: None = Depends(require_admin)):
    db = load_db()
    return {"ok": True, "items": [map_link_for_admin(x) for x in db.get("links", [])]}


@app.get("/api/admin/groups")
async def api_admin_groups(_: None = Depends(require_admin)):
    db = load_db()
    return {"ok": True, "groups": build_groups(db.get("links", []))}


@app.delete("/api/admin/link/{link_id}")
async def api_admin_delete_link(link_id: str, _: None = Depends(require_admin)):
    db = load_db()
    before = len(db.get("links", []))
    db["links"] = [x for x in db.get("links", []) if x.get("id") != link_id]
    if len(db["links"]) == before:
        return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
    save_db(db)
    return {"ok": True}


@app.delete("/api/admin/link/{link_id}/activations/{index}")
async def api_admin_delete_activation(link_id: str, index: int, _: None = Depends(require_admin)):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("id") == link_id), None)
    if not item:
        return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
    activations = item.get("activations", [])
    if index < 0 or index >= len(activations):
        return JSONResponse({"ok": False, "error": "Активация не найдена"}, status_code=400)
    activations.pop(index)
    total_used = len(item.get("activations", [])) + len(item.get("violations", []))
    if item.get("violations"):
        item["status"] = "violator"
    elif total_used >= int(item.get("maxActivations", 0)):
        item["status"] = "used"
    else:
        item["status"] = "active"
    last_a = item.get("activations", [])[-1]["at"] if item.get("activations") else None
    last_v = item.get("violations", [])[-1]["at"] if item.get("violations") else None
    item["lastUsedAt"] = last_a or last_v
    save_db(db)
    return {"ok": True}


@app.delete("/api/admin/link/{link_id}/violations/{index}")
async def api_admin_delete_violation(link_id: str, index: int, _: None = Depends(require_admin)):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("id") == link_id), None)
    if not item:
        return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
    violations = item.get("violations", [])
    if index < 0 or index >= len(violations):
        return JSONResponse({"ok": False, "error": "Нарушение не найдено"}, status_code=400)
    violations.pop(index)
    total_used = len(item.get("activations", [])) + len(item.get("violations", []))
    if item.get("violations"):
        item["status"] = "violator"
    elif total_used >= int(item.get("maxActivations", 0)):
        item["status"] = "used"
    else:
        item["status"] = "active"
    last_a = item.get("activations", [])[-1]["at"] if item.get("activations") else None
    last_v = item.get("violations", [])[-1]["at"] if item.get("violations") else None
    item["lastUsedAt"] = last_a or last_v
    save_db(db)
    return {"ok": True}


@app.delete("/api/admin/link/{link_id}/reset")
async def api_admin_reset(link_id: str, _: None = Depends(require_admin)):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("id") == link_id), None)
    if not item:
        return JSONResponse({"ok": False, "error": "Ссылка не найдена"}, status_code=404)
    item["activations"] = []
    item["violations"] = []
    item["usedCount"] = 0
    item["lastUsedAt"] = None
    item["status"] = "active"
    save_db(db)
    return {"ok": True}


def make_qr_response(full_url: str) -> Response:
    img = qrcode.make(full_url)
    from io import BytesIO
    buf = BytesIO()
    img.save(buf, format="PNG")
    return Response(buf.getvalue(), media_type="image/png", headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/api/qrcode/{token}")
async def api_qrcode(token: str, request: Request):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("token") == token), None)
    if not item:
        return JSONResponse({"ok": False, "error": "Токен не найден"}, status_code=404)
    base_url = f"{request.url.scheme}://{request.headers.get('host')}"
    return make_qr_response(f"{base_url}/r/{token}")


@app.get("/r/{token}")
async def route_redeem(token: str):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("token") == token), None)
    if not item:
        return FileResponse(PUBLIC_DIR / "invalid.html")
    total_used = len(item.get("activations", [])) + len(item.get("violations", []))
    if total_used >= int(item.get("maxActivations", 0)):
        return FileResponse(PUBLIC_DIR / "used.html")
    return FileResponse(PUBLIC_DIR / "redeem.html")


# ==================== МАРШРУТЫ С ПРЕФИКСОМ /happ ====================

@app.get("/happ/")
async def happ_root():
    return FileResponse(PUBLIC_DIR / "index.html")


@app.get("/happ/admin.html")
async def happ_admin_html(_: None = Depends(require_admin)):
    return FileResponse(PUBLIC_DIR / "admin.html")


@app.post("/happ/api/generate")
async def happ_api_generate(payload: GenerateRequest):
    return await api_generate(payload)


@app.get("/happ/api/link/{token}")
async def happ_api_link(token: str):
    return await api_link(token)


@app.post("/happ/api/check-device/{token}")
async def happ_api_check_device(token: str, request: Request):
    return await api_check_device(token, request)


@app.post("/happ/api/redeem-preview/{token}")
async def happ_api_redeem_preview(token: str, request: Request):
    return await api_redeem_preview(token, request)


@app.post("/happ/api/redeem-confirm/{token}")
async def happ_api_redeem_confirm(token: str, request: Request):
    return await api_redeem_confirm(token, request)


@app.get("/happ/api/admin/links")
async def happ_api_admin_links(_: None = Depends(require_admin)):
    return await api_admin_links(_)


@app.get("/happ/api/admin/groups")
async def happ_api_admin_groups(_: None = Depends(require_admin)):
    return await api_admin_groups(_)


@app.delete("/happ/api/admin/link/{link_id}")
async def happ_api_admin_delete_link(link_id: str, _: None = Depends(require_admin)):
    return await api_admin_delete_link(link_id, _)


@app.delete("/happ/api/admin/link/{link_id}/activations/{index}")
async def happ_api_admin_delete_activation(link_id: str, index: int, _: None = Depends(require_admin)):
    return await api_admin_delete_activation(link_id, index, _)


@app.delete("/happ/api/admin/link/{link_id}/violations/{index}")
async def happ_api_admin_delete_violation(link_id: str, index: int, _: None = Depends(require_admin)):
    return await api_admin_delete_violation(link_id, index, _)


@app.delete("/happ/api/admin/link/{link_id}/reset")
async def happ_api_admin_reset(link_id: str, _: None = Depends(require_admin)):
    return await api_admin_reset(link_id, _)


@app.get("/happ/api/qrcode/{token}")
async def happ_api_qrcode(token: str, request: Request):
    db = load_db()
    item = next((x for x in db.get("links", []) if x.get("token") == token), None)
    if not item:
        return JSONResponse({"ok": False, "error": "Токен не найден"}, status_code=404)
    base_url = f"{request.url.scheme}://{request.headers.get('host')}"
    return make_qr_response(f"{base_url}/happ/r/{token}")


@app.get("/happ/r/{token}")
async def happ_route_redeem(token: str):
    return await route_redeem(token)


@app.get("/happ/static/{path:path}")
async def happ_static(path: str):
    file_path = PUBLIC_DIR / path
    if file_path.exists() and file_path.is_file():
        return FileResponse(file_path)
    return JSONResponse({"ok": False, "error": "File not found"}, status_code=404)


# Статические файлы для корневого пути
app.mount("/static", StaticFiles(directory=str(PUBLIC_DIR)), name="static")

# Статические файлы для /happ/static (чтобы работали ссылки на CSS/JS в HTML)
app.mount("/happ/static", StaticFiles(directory=str(PUBLIC_DIR)), name="happ_static")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def fallback(path: str, request: Request):
    return JSONResponse({"ok": False, "error": f"Маршрут не найден: {request.method} /{path}"}, status_code=404)
