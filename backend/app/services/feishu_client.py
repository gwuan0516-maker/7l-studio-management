"""飞书开放平台 API 客户端"""
import time
import httpx
from app.config import settings


class FeishuClient:
    """飞书开放平台 API 封装"""

    BASE_URL = "https://open.feishu.cn/open-apis"

    def __init__(self):
        self._tenant_token: str = ""
        self._token_expires: float = 0
        self._client = httpx.AsyncClient(timeout=30)

    async def _ensure_token(self):
        """确保 tenant_access_token 有效"""
        if self._tenant_token and time.time() < self._token_expires - 60:
            return
        url = f"{self.BASE_URL}/auth/v3/tenant_access_token/internal"
        resp = await self._client.post(url, json={
            "app_id": settings.FEISHU_APP_ID,
            "app_secret": settings.FEISHU_APP_SECRET,
        })
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"获取飞书token失败: {data}")
        self._tenant_token = data["tenant_access_token"]
        self._token_expires = time.time() + data.get("expire", 7200)

    async def _headers(self) -> dict:
        await self._ensure_token()
        return {"Authorization": f"Bearer {self._tenant_token}", "Content-Type": "application/json"}

    # ── 消息发送 ──────────────────────────────────────────

    async def send_text(self, chat_id: str, text: str):
        """发送文本消息到群聊"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        body = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": f'{{"text":"{text}"}}',
        }
        resp = await self._client.post(url, headers=headers, params=params, json=body)
        return resp.json()

    async def send_rich_text(self, chat_id: str, content: dict):
        """发送富文本消息"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        import json
        body = {
            "receive_id": chat_id,
            "msg_type": "post",
            "content": json.dumps(content),
        }
        resp = await self._client.post(url, headers=headers, params=params, json=body)
        return resp.json()

    async def reply_message(self, message_id: str, text: str):
        """回复消息"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/im/v1/messages/{message_id}/reply"
        body = {
            "msg_type": "text",
            "content": f'{{"text":"{text}"}}',
        }
        resp = await self._client.post(url, headers=headers, json=body)
        return resp.json()

    # ── 图片下载 ──────────────────────────────────────────

    async def download_image(self, image_key: str) -> bytes:
        """下载飞书图片，返回原始字节"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/im/v1/images/{image_key}"
        resp = await self._client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.content

    # ── 事件验证 ──────────────────────────────────────────

    @staticmethod
    def verify_event(data: dict, challenge: bool = False) -> dict:
        """处理飞书事件订阅验证（URL验证）"""
        if challenge and "challenge" in data:
            return {"challenge": data["challenge"]}
        return data


# 全局单例
feishu_client = FeishuClient()
