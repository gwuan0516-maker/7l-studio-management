"""飞书事件订阅 Webhook 路由"""
import json
import hashlib
import hmac
import base64
import time
import logging
from fastapi import APIRouter, Request, Header, HTTPException
from app.services.command_parser import parse_command, CommandType
from app.services.command_handler import handle_command, handle_ocr_image
from app.services.feishu_client import feishu_client
from app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


def _verify_feishu_signature(timestamp: str, body: bytes, signature: str) -> bool:
    """验证飞书事件签名
    签名算法: Base64(HMAC-SHA256(webhook_secret, timestamp + "\n" + body))
    """
    webhook_secret = settings.WEBHOOK_SECRET
    if not webhook_secret:
        logger.warning("WEBHOOK_SECRET 未配置，跳过签名验证")
        return True

    if not timestamp or not signature:
        return False

    # 拼接待签名字符串: timestamp + "\n" + body
    string_to_sign = f"{timestamp}\n".encode("utf-8") + body

    # HMAC-SHA256
    hmac_code = hmac.new(
        webhook_secret.encode("utf-8"),
        string_to_sign,
        hashlib.sha256,
    ).digest()

    # Base64 编码
    expected_signature = base64.b64encode(hmac_code).decode("utf-8")

    return hmac.compare_digest(expected_signature, signature)


@router.post("/webhook/feishu")
async def feishu_webhook(
    request: Request,
    x_lark_signature: str = Header(None, alias="X-Lark-Signature"),
    x_lark_request_timestamp: str = Header(None, alias="X-Lark-Request-Timestamp"),
):
    """飞书事件回调入口"""
    # 读取原始 body（签名验证需要原始字节）
    body_bytes = await request.body()

    # 签名验证
    if not _verify_feishu_signature(x_lark_request_timestamp, body_bytes, x_lark_signature):
        logger.warning("飞书 webhook 签名验证失败")
        raise HTTPException(status_code=403, detail="签名验证失败")

    body = json.loads(body_bytes)

    # URL验证（首次配置时飞书会发验证请求）
    if "challenge" in body:
        return {"challenge": body["challenge"]}

    # 事件回调
    header = body.get("header", {})
    event_type = header.get("event_type", "")

    # 只处理消息事件
    if event_type != "im.message.receive_v1":
        return {"ok": True}

    event = body.get("event", {})
    message = event.get("message", {})
    msg_type = message.get("message_type", "")
    chat_id = message.get("chat_id", "")
    message_id = message.get("message_id", "")
    sender = event.get("sender", {})
    sender_id = sender.get("sender_id", {})
    user_id = sender_id.get("user_id", "")

    # 避免处理自己发的消息
    if sender.get("sender_type") == "app":
        return {"ok": True}

    try:
        if msg_type == "text":
            # 文本消息 → 命令解析
            content = json.loads(message.get("content", "{}"))
            text = content.get("text", "").strip()
            if not text:
                return {"ok": True}

            logger.info(f"收到消息: chat_id={chat_id} text={text[:50]}")

            cmd = parse_command(text)
            reply = await handle_command(cmd, operator=user_id or "店长", chat_id=chat_id)

            # 回复消息
            await feishu_client.reply_message(message_id, reply)

        elif msg_type == "image":
            # 图片消息 → OCR识别
            content = json.loads(message.get("content", "{}"))
            image_key = content.get("image_key", "")
            if image_key:
                logger.info(f"收到图片: chat_id={chat_id} image_key={image_key}")
                reply = await handle_ocr_image(image_key, operator=user_id or "店长")
                await feishu_client.reply_message(message_id, reply)

        else:
            logger.info(f"忽略消息类型: {msg_type}")

    except Exception as e:
        logger.error(f"处理消息失败: {e}", exc_info=True)
        try:
            await feishu_client.reply_message(message_id, f"⚠️ 处理失败: {str(e)[:100]}")
        except Exception:
            pass

    return {"ok": True}


@router.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "ok", "service": "7l-bot"}
