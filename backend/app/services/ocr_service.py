"""百度OCR服务 - 识别接龙截图"""
import base64
import time
import httpx
from app.config import settings


class BaiduOCRService:
    """百度智能云 OCR 封装"""

    TOKEN_URL = "https://aip.baidubce.com/oauth/2.0/token"
    OCR_URL = "https://aip.baidubce.com/rest/2.0/ocr/v1/accurate_basic"

    def __init__(self):
        self._access_token: str = ""
        self._token_expires: float = 0
        self._client = httpx.AsyncClient(timeout=30)

    async def _ensure_token(self):
        """确保百度access_token有效"""
        if self._access_token and time.time() < self._token_expires - 60:
            return
        resp = await self._client.post(self.TOKEN_URL, params={
            "grant_type": "client_credentials",
            "client_id": settings.BAIDU_OCR_API_KEY,
            "client_secret": settings.BAIDU_OCR_SECRET_KEY,
        })
        data = resp.json()
        self._access_token = data["access_token"]
        self._token_expires = time.time() + data.get("expires_in", 2592000)

    async def recognize_image(self, image_bytes: bytes) -> list[str]:
        """识别图片中的文字，返回行列表"""
        await self._ensure_token()
        img_b64 = base64.b64encode(image_bytes).decode()

        resp = await self._client.post(
            self.OCR_URL,
            params={"access_token": self._access_token},
            data={"image": img_b64, "detect_direction": "true"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        data = resp.json()
        words_result = data.get("words_result", [])
        return [item["words"] for item in words_result]

    def parse_jielong_text(self, lines: list[str]) -> list[dict]:
        """
        解析接龙截图文本，提取学员信息。
        
        接龙格式通常为：
        1. 张三 体验课
        2. 李四 次卡
        3. 王五 +1
        
        返回: [{"name": "张三", "note": "体验课"}, ...]
        """
        results = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # 匹配 "数字. 名字 备注" 或 "数字、名字 备注"
            import re
            m = re.match(r'^\d+[.、\s]+(.+)', line)
            if m:
                content = m.group(1).strip()
                # 尝试分离名字和备注
                parts = content.split(None, 1)
                name = parts[0] if parts else content
                note = parts[1] if len(parts) > 1 else ""
                # 过滤掉纯数字（+1之类）
                if name and not name.isdigit() and name not in ("+1", "+2"):
                    results.append({"name": name, "note": note})
        return results


# 全局单例
ocr_service = BaiduOCRService()
