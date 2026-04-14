import json
import requests
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import uuid
import urllib.parse


class XUIManager:
    def __init__(self, config_path: str = "servers_config.json"):
        with open(config_path, "r", encoding="utf-8") as f:
            self.config = json.load(f)
        self.servers = {s["id"]: s for s in self.config["servers"]}
        self.sessions = {}

    def _get_session(self, server_id: str) -> requests.Session:
        """Получить сессию с авторизацией для сервера"""
        if server_id in self.sessions:
            return self.sessions[server_id]

        server = self.servers.get(server_id)
        if not server:
            raise ValueError(f"Server {server_id} not found")

        session = requests.Session()
        # Игнорируем SSL ошибки для самоподписанных сертификатов
        session.verify = False
        
        # Логин в 3x-ui
        login_url = f"{server['address']}/login"
        login_data = {
            "username": server["username"],
            "password": server["password"]
        }
        
        try:
            response = session.post(login_url, json=login_data, timeout=10)
            if response.status_code != 200:
                raise Exception(f"Login failed: {response.text}")
        except Exception as e:
            raise Exception(f"Cannot connect to {server['name']}: {e}")

        self.sessions[server_id] = session
        return session

    def list_inbounds(self, server_id: str) -> List[Dict]:
        """Получить список всех inbound'ов с сервера"""
        session = self._get_session(server_id)
        server = self.servers[server_id]
        
        list_url = f"{server['address']}/xui/API/inbounds/list"
        
        try:
            response = session.get(list_url, timeout=10)
            
            if response.status_code != 200:
                return self._get_fallback_inbounds(server)
            
            data = response.json()
            if not data.get("success"):
                return self._get_fallback_inbounds(server)
            
            inbounds = data.get("obj", [])
            
            # Фильтруем inbound'ы если указаны в конфиге
            allowed_inbounds = server.get("inbounds")
            if allowed_inbounds and isinstance(allowed_inbounds, list):
                inbounds = [ib for ib in inbounds if ib.get("id") in allowed_inbounds]
            
            # Форматируем для отображения
            result = []
            for ib in inbounds:
                result.append({
                    "id": ib.get("id"),
                    "name": ib.get("remark") or f"{ib.get('protocol', 'unknown')}:{ib.get('port', 0)}",
                    "protocol": ib.get("protocol"),
                    "port": ib.get("port"),
                    "streamSettings": ib.get("streamSettings"),
                    "settings": ib.get("settings"),
                    "default": server.get("defaultInbound") == ib.get("id")
                })
            
            return result
            
        except Exception as e:
            print(f"Error loading inbounds: {e}")
            return self._get_fallback_inbounds(server)
    
    def _get_fallback_inbounds(self, server) -> List[Dict]:
        """Возвращает inbound'ы из конфига если API не доступен"""
        allowed = server.get("inbounds", [])
        if not allowed:
            return []
        
        return [{"id": ib_id, "name": f"Inbound {ib_id}", "default": server.get("defaultInbound") == ib_id} 
                for ib_id in allowed]

    def create_client(
        self,
        server_id: str,
        inbound_id: int,
        email: str,
        traffic_gb: int = 100,
        expiry_days: int = 30,
        enable: bool = True
    ) -> Dict[str, Any]:
        """Создать клиента на сервере"""
        session = self._get_session(server_id)
        server = self.servers[server_id]
        
        # Вычисляем дату окончания
        expiry_time = datetime.now() + timedelta(days=expiry_days)
        expiry_timestamp = int(expiry_time.timestamp() * 1000)
        
        # Конвертируем трафик в байты
        total_bytes = traffic_gb * 1024 * 1024 * 1024
        
        # Генерируем UUID для клиента
        client_uuid = str(uuid.uuid4())
        
        # Сначала получаем текущий inbound, чтобы узнать его настройки
        get_url = f"{server['address']}/xui/API/inbounds/get/{inbound_id}"
        response = session.get(get_url, timeout=10)
        
        if response.status_code != 200:
            raise Exception(f"Failed to get inbound {inbound_id}")
        
        inbound_data = response.json()
        if not inbound_data.get("success"):
            raise Exception(f"API error: {inbound_data}")
        
        inbound_obj = inbound_data.get("obj", {})
        settings = json.loads(inbound_obj.get("settings", "{}"))
        
        # Добавляем нового клиента
        clients = settings.get("clients", [])
        clients.append({
            "id": client_uuid,
            "email": email,
            "limitIp": 0,
            "totalGB": total_bytes,
            "expiryTime": expiry_timestamp,
            "enable": enable,
            "tgId": "",
            "subId": ""
        })
        
        settings["clients"] = clients
        
        # Обновляем inbound с новым клиентом
        update_data = {
            "id": inbound_id,
            "settings": json.dumps(settings)
        }
        
        update_url = f"{server['address']}/xui/API/inbounds/update/{inbound_id}"
        response = session.post(update_url, json=update_data, timeout=10)
        
        if response.status_code != 200:
            raise Exception(f"Failed to create client: {response.text}")
        
        result = response.json()
        if result.get("success") != True:
            raise Exception(f"API error: {result}")
        
        # Получаем ссылку для подключения
        link = self._get_client_link(server_id, inbound_id, client_uuid, inbound_obj)
        
        # Получаем subscription URL
        sub_url = f"{server['address']}/subscribe/{client_uuid}"
        
        return {
            "client_id": client_uuid,
            "email": email,
            "traffic_gb": traffic_gb,
            "expiry_date": expiry_time.isoformat(),
            "inbound_id": inbound_id,
            "inbound_name": inbound_obj.get("remark", f"Inbound {inbound_id}"),
            "server_name": server["name"],
            "link": link,
            "subscription_url": sub_url
        }
    
    def _get_client_link(self, server_id: str, inbound_id: int, client_id: str, inbound_obj: Dict) -> str:
        """Получить ссылку для подключения клиента"""
        server = self.servers[server_id]
        protocol = inbound_obj.get("protocol", "")
        port = inbound_obj.get("port", 443)
        stream_settings = json.loads(inbound_obj.get("streamSettings", "{}"))
        settings = json.loads(inbound_obj.get("settings", "{}"))
        
        # Находим клиента
        client = None
        for c in settings.get("clients", []):
            if c.get("id") == client_id:
                client = c
                break
        
        if not client:
            return ""
        
        # Парсим адрес сервера
        addr_clean = server["address"].replace("https://", "").replace("http://", "").split(":")[0]
        
        # Формируем ссылку в зависимости от протокола
        if protocol == "vless":
            return self._build_vless_link(addr_clean, port, client, stream_settings)
        elif protocol == "vmess":
            return self._build_vmess_link(addr_clean, port, client, stream_settings)
        elif protocol == "trojan":
            return self._build_trojan_link(addr_clean, port, client, stream_settings)
        elif protocol == "shadowsocks":
            return self._build_ss_link(addr_clean, port, client, stream_settings)
        
        return f"{protocol}://{client_id}@{addr_clean}:{port}"
    
    def _build_vless_link(self, address: str, port: int, client: Dict, stream: Dict) -> str:
        """Построить VLESS ссылку"""
        security = stream.get("security", "none")
        network = stream.get("network", "tcp")
        
        params = {
            "encryption": client.get("encryption", "none"),
            "security": security,
            "type": network,
            "sni": stream.get("settings", {}).get("serverName", ""),
            "fp": "chrome"
        }
        
        if network == "ws":
            ws_settings = stream.get("wsSettings", {})
            params["path"] = ws_settings.get("path", "/")
            params["host"] = ws_settings.get("headers", {}).get("Host", "")
        elif network == "grpc":
            grpc_settings = stream.get("grpcSettings", {})
            params["serviceName"] = grpc_settings.get("serviceName", "")
        
        # Убираем пустые параметры
        params = {k: v for k, v in params.items() if v}
        
        query = urllib.parse.urlencode(params)
        fragment = client.get("email", "")
        
        return f"vless://{client['id']}@{address}:{port}?{query}#{fragment}"
    
    def _build_vmess_link(self, address: str, port: int, client: Dict, stream: Dict) -> str:
        """Построить VMESS ссылку"""
        import base64
        
        vmess_config = {
            "v": "2",
            "ps": client.get("email", ""),
            "add": address,
            "port": port,
            "id": client["id"],
            "aid": "0",
            "net": stream.get("network", "tcp"),
            "type": "none",
            "host": "",
            "path": "",
            "tls": "tls" if stream.get("security") == "tls" else ""
        }
        
        # Добавляем параметры для WebSocket
        if vmess_config["net"] == "ws":
            ws_settings = stream.get("wsSettings", {})
            vmess_config["path"] = ws_settings.get("path", "/")
            vmess_config["host"] = ws_settings.get("headers", {}).get("Host", "")
        
        # Добавляем параметры для gRPC
        if vmess_config["net"] == "grpc":
            grpc_settings = stream.get("grpcSettings", {})
            vmess_config["path"] = grpc_settings.get("serviceName", "")
        
        vmess_json = json.dumps(vmess_config, separators=(",", ":"))
        vmess_b64 = base64.b64encode(vmess_json.encode()).decode()
        
        return f"vmess://{vmess_b64}"
    
    def _build_trojan_link(self, address: str, port: int, client: Dict, stream: Dict) -> str:
        """Построить Trojan ссылку"""
        password = client.get("password", client.get("id", ""))
        sni = stream.get("settings", {}).get("serverName", "")
        
        params = []
        if sni:
            params.append(f"sni={sni}")
        
        query = "?" + "&".join(params) if params else ""
        fragment = client.get("email", "")
        
        return f"trojan://{password}@{address}:{port}{query}#{fragment}"
    
    def _build_ss_link(self, address: str, port: int, client: Dict, stream: Dict) -> str:
        """Построить Shadowsocks ссылку"""
        import base64
        
        method = stream.get("method", "chacha20-ietf-poly1305")
        password = client.get("password", client.get("id", ""))
        
        userinfo = f"{method}:{password}"
        userinfo_b64 = base64.b64encode(userinfo.encode()).decode()
        
        fragment = client.get("email", "")
        
        return f"ss://{userinfo_b64}@{address}:{port}#{fragment}"
    
    def get_client_status(self, server_id: str, client_id: str) -> Dict:
        """Получить статус клиента (трафик, срок действия)"""
        session = self._get_session(server_id)
        server = self.servers[server_id]
        
        # Получаем список всех inbound'ов
        inbounds = self.list_inbounds(server_id)
        
        for inbound in inbounds:
            inbound_id = inbound["id"]
            get_url = f"{server['address']}/xui/API/inbounds/get/{inbound_id}"
            response = session.get(get_url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success"):
                    inbound_obj = data.get("obj", {})
                    settings = json.loads(inbound_obj.get("settings", "{}"))
                    
                    for client in settings.get("clients", []):
                        if client.get("id") == client_id:
                            total_gb = client.get("totalGB", 0) / (1024**3)
                            used_gb = client.get("up", 0) / (1024**3) + client.get("down", 0) / (1024**3)
                            remaining_gb = max(0, total_gb - used_gb)
                            expiry = datetime.fromtimestamp(client.get("expiryTime", 0) / 1000)
                            
                            return {
                                "found": True,
                                "client_id": client_id,
                                "email": client.get("email"),
                                "total_gb": round(total_gb, 2),
                                "used_gb": round(used_gb, 2),
                                "remaining_gb": round(remaining_gb, 2),
                                "expiry_date": expiry.isoformat(),
                                "enable": client.get("enable", True),
                                "inbound_id": inbound_id,
                                "inbound_name": inbound_obj.get("remark")
                            }
        
        return {"found": False}
