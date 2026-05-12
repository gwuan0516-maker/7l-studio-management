"""OpenClaw Gateway WebSocket客户端"""

import uuid
import json
import logging
import asyncio
from typing import Optional

import websockets

logger = logging.getLogger(__name__)


class OpenClawGatewayClient:
    """OpenClaw Gateway WebSocket客户端，长连接复用"""

    def __init__(self, url: str, token: str, agent_id: str = "7l-bot"):
        self.url = url
        self.token = token
        self.agent_id = agent_id
        self.ws = None
        self._connected = False
        self._connect_lock = asyncio.Lock()

    async def connect(self) -> bool:
        """建立连接+握手认证"""
        # 1. 建立WebSocket连接（绕过系统代理，本地连接不需要代理）
        self.ws = await websockets.connect(self.url, proxy=None)

        # 2. 等待connect.challenge事件
        challenge_frame = json.loads(await self.ws.recv())
        if challenge_frame.get("event") != "connect.challenge":
            raise ConnectionError(f"Expected connect.challenge, got {challenge_frame}")

        # 3. 发送connect请求（认证）
        connect_req = {
            "type": "req",
            "id": str(uuid.uuid4()),
            "method": "connect",
            "params": {
                "minProtocol": 3,
                "maxProtocol": 3,
                "client": {
                    "id": "gateway-client",
                    "version": "1.0.0",
                    "platform": "python",
                    "mode": "backend",
                },
                "auth": {
                    "token": self.token,
                },
                "role": "operator",
                "scopes": [
                    "operator.admin",
                    "operator.read",
                    "operator.write",
                    "operator.approvals",
                    "operator.pairing",
                ],
            },
        }
        await self.ws.send(json.dumps(connect_req))

        # 4. 等待connect响应
        connect_res = json.loads(await self.ws.recv())
        if not connect_res.get("ok"):
            raise ConnectionError(f"Gateway auth failed: {connect_res}")

        self._connected = True
        logger.info("OpenClaw Gateway connected and authenticated")
        return True

    async def ensure_connected(self) -> bool:
        """确保连接可用，断线自动重连"""
        if self._connected and self.ws and self.ws.state == 1:  # OPEN=1
            return True
        async with self._connect_lock:
            try:
                return await self.connect()
            except Exception as e:
                logger.error(f"Gateway reconnect failed: {e}")
                self._connected = False
                return False

    async def chat(
        self,
        message: str,
        session_key: str = "agent:leo:7l-bot",
        timeout: float = 20.0,
    ) -> Optional[str]:
        """发送消息并等待AI回复"""
        if not await self.ensure_connected():
            return None

        req_id = str(uuid.uuid4())

        # 发送chat.send请求
        req = {
            "type": "req",
            "id": req_id,
            "method": "chat.send",
            "params": {
                "sessionKey": session_key,
                "message": message,
                "idempotencyKey": str(uuid.uuid4()),
            },
        }

        try:
            await self.ws.send(json.dumps(req))

            # 等待响应：res帧(accepted) → event帧(final)
            ai_reply = None
            loop = asyncio.get_event_loop()
            deadline = loop.time() + timeout

            while loop.time() < deadline:
                remaining = deadline - loop.time()
                raw = await asyncio.wait_for(self.ws.recv(), timeout=remaining)
                frame = json.loads(raw)

                # res帧：确认已接受
                if frame.get("type") == "res" and frame.get("id") == req_id:
                    if not frame.get("ok"):
                        logger.error(f"chat.send failed: {frame}")
                        return None
                    continue

                # event帧：AI回复
                if frame.get("type") == "event" and frame.get("event") == "chat":
                    state = frame.get("payload", {}).get("state")
                    if state == "final":
                        msg = frame.get("payload", {}).get("message")
                        logger.info(f"Gateway final message type: {type(msg)}, value: {str(msg)[:500]}")
                        # message可能是dict/list/string
                        if isinstance(msg, dict):
                            content = msg.get("content", msg.get("text", str(msg)))
                            if isinstance(content, list):
                                # OpenAI格式: [{type: text, text: "..."}]
                                ai_reply = " ".join(c.get("text", "") for c in content if isinstance(c, dict))
                            else:
                                ai_reply = str(content)
                        elif isinstance(msg, list):
                            # 直接是content数组
                            ai_reply = " ".join(c.get("text", str(c)) for c in msg if isinstance(c, dict))
                        else:
                            ai_reply = str(msg) if msg else ""
                        break
                    # delta帧可以忽略（不做流式）
                    continue

            return ai_reply

        except asyncio.TimeoutError:
            logger.warning("Gateway chat timeout")
            self._connected = False  # 标记断连，下次重连
            return None
        except websockets.ConnectionClosed:
            logger.warning("Gateway connection closed")
            self._connected = False
            return None
        except Exception as e:
            logger.error(f"Gateway chat error: {e}")
            self._connected = False
            return None

    async def close(self):
        """关闭连接"""
        if self.ws:
            await self.ws.close()
            self._connected = False
