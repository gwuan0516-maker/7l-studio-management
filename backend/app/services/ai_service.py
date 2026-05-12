"""AI助手服务 - 意图识别+参数提取+多轮对话+执行"""

import os
import re
import time
import json
import uuid
import logging
import asyncio
from datetime import datetime
from typing import Optional

import asyncio
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ── 11种意图 ──────────────────────────────────────────

INTENTS = [
    "register",    # 录入学员
    "deduct",      # 扣课签到
    "query",       # 查询学员
    "stats",       # 统计
    "renew",       # 续费
    "refund",      # 退卡
    "update",      # 修改信息
    "import",      # 批量导入
    "freeze",      # 请假/冻结
    "undo",        # 撤销操作
    "multi",       # 多意图组合
]

# ── 意图描述（给DeepSeek的prompt用） ──────────────────

INTENT_DESCRIPTIONS = """
1. register - 录入新学员。必要参数：姓名、课次、金额。可选参数：卡种名称、付款方式、电话、微信昵称、渠道来源、备注。注意：卡种名称为可选标签，课次和金额允许为0但不能同时为0。如果用户提到卡种名称（如"次卡16次"），提取为卡种名称参数；如果只说"16次课980元"，则卡种名称为空。
2. deduct - 扣课签到。必要参数：姓名。可选参数：扣课次数(默认1)、老师、舞种。
3. query - 查询学员信息。必要参数：姓名。
4. stats - 查看统计数据。无需参数。
5. renew - 续费。必要参数：姓名、卡种名称。可选参数：金额、付款方式。
6. refund - 退卡。必要参数：姓名。
7. update - 修改学员信息。必要参数：姓名。可选参数：电话、微信昵称、备注。
8. import - 批量导入学员。说明用户想批量导入，引导使用导入功能。
9. freeze - 请假/冻结。必要参数：姓名。可选参数：天数(默认7)。
10. undo - 撤销最近操作。可选参数：姓名。
11. multi - 多意图组合。当用户一句话包含多个操作时使用。
"""

# ── 卡种列表（会在运行时从API加载） ──────────────────

_card_prices_cache = {"data": [], "ts": 0}

# ── Session管理 ────────────────────────────────────────

class SessionManager:
    """内存session管理，10分钟超时"""

    def __init__(self, timeout_seconds: int = 600):
        self.sessions: dict = {}
        self.timeout = timeout_seconds
        self._cleanup_task: Optional[asyncio.Task] = None

    def create_session(self) -> str:
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = {
            "created_at": time.time(),
            "last_active": time.time(),
            "history": [],       # [{"role": "user"/"ai", "content": "..."}]
            "state": "idle",     # idle, collecting, confirming
            "pending_action": None,  # {"type": "register", "params": {...}}
            "context": {},       # 额外上下文
        }
        return session_id

    def get_session(self, session_id: str) -> Optional[dict]:
        session = self.sessions.get(session_id)
        if not session:
            return None
        # 检查超时
        if time.time() - session["last_active"] > self.timeout:
            del self.sessions[session_id]
            return None
        session["last_active"] = time.time()
        return session

    def update_session(self, session_id: str, updates: dict):
        session = self.sessions.get(session_id)
        if session:
            session.update(updates)
            session["last_active"] = time.time()

    def delete_session(self, session_id: str):
        self.sessions.pop(session_id, None)

    async def cleanup_loop(self):
        """定期清理过期session"""
        while True:
            await asyncio.sleep(60)
            now = time.time()
            expired = []
            for sid, s in self.sessions.items():
                if now - s["last_active"] > self.timeout:
                    expired.append(sid)
            for sid in expired:
                del self.sessions[sid]
                logger.info(f"Session {sid} expired and cleaned up")

    def start_cleanup(self):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self.cleanup_loop())


session_manager = SessionManager()

# ── 频率限制 ──────────────────────────────────────────

class RateLimiter:
    """同一类型操作1分钟内不超过5次"""

    def __init__(self, max_calls: int = 5, window_seconds: int = 60):
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls: dict[str, list[float]] = {}

    def check(self, intent_type: str) -> bool:
        """返回True表示允许，False表示超限"""
        now = time.time()
        if intent_type not in self.calls:
            self.calls[intent_type] = []

        # 清理过期记录
        self.calls[intent_type] = [t for t in self.calls[intent_type] if now - t < self.window]

        if len(self.calls[intent_type]) >= self.max_calls:
            return False

        self.calls[intent_type].append(now)
        return True


rate_limiter = RateLimiter()

# ── 数据脱敏 ──────────────────────────────────────────

def desensitize_phone(phone: str) -> str:
    """手机号脱敏：138****1234"""
    if not phone:
        return ""
    phone = phone.strip()
    if re.match(r'^1\d{10}$', phone):
        return phone[:3] + "****" + phone[-4:]
    return phone


def desensitize_data(data: dict) -> dict:
    """脱敏数据中的手机号"""
    if not data:
        return data
    result = {}
    for k, v in data.items():
        if isinstance(v, str):
            result[k] = desensitize_phone(v) if re.match(r'^1\d{10}$', v) else v
        elif isinstance(v, dict):
            result[k] = desensitize_data(v)
        else:
            result[k] = v
    return result


# ── OpenClaw Gateway AI调用 ──────────────────────────────

# 模块级Gateway客户端实例
_gateway_client = None


async def call_llm(messages: list[dict], timeout: float = 20.0) -> Optional[str]:
    """通过OpenClaw Gateway调用AI（复用已付费的模型，零额外成本）"""
    global _gateway_client

    gateway_url = os.getenv("OPENCLAW_GATEWAY_URL", "ws://127.0.0.1:18789")
    gateway_token = os.getenv("OPENCLAW_GATEWAY_TOKEN", "")

    if not gateway_token:
        logger.warning("OPENCLAW_GATEWAY_TOKEN not configured, AI功能不可用")
        return None

    # 懒初始化Gateway客户端
    if _gateway_client is None:
        from app.services.openclaw_gateway import OpenClawGatewayClient
        _gateway_client = OpenClawGatewayClient(gateway_url, gateway_token)

    # 构造消息：把messages列表合并为一条消息发给Gateway
    # 只发最新消息，上下文由Gateway session管理
    latest_message = messages[-1]["content"] if messages else ""

    # 调用Gateway
    result = await _gateway_client.chat(latest_message, timeout=timeout)

    # 失败重试1次
    if result is None:
        logger.info("Gateway call failed, retrying once...")
        result = await _gateway_client.chat(latest_message, timeout=timeout)

    return result


# ── 意图识别+参数提取 ──────────────────────────────────

async def recognize_intent(
    message: str,
    session: Optional[dict] = None,
    card_prices: list[dict] = None,
) -> dict:
    """
    识别用户意图并提取参数。
    返回: {
        "intent": str,
        "params": dict,
        "reply": str,
        "need_confirm": bool,
        "missing_params": list[str],
    }
    """
    # 构建卡种列表描述（仅作参考提示，不强制匹配）
    card_list_desc = ""
    if card_prices:
        card_names = [p.get("name", "") for p in card_prices if p.get("name")]
        card_list_desc = f"\n\n参考卡种列表（仅作参考，不强制匹配）：{', '.join(card_names)}"

    # 构建对话历史
    history_str = ""
    if session and session.get("history"):
        recent = session["history"][-6:]  # 最近3轮
        for h in recent:
            role = "用户" if h["role"] == "user" else "AI"
            history_str += f"{role}: {h['content']}\n"
        if history_str:
            history_str = f"\n\n对话历史：\n{history_str}"

    # 当前pending状态
    pending_str = ""
    if session and session.get("pending_action"):
        pending_str = f"\n\n当前待确认操作：{json.dumps(session['pending_action'], ensure_ascii=False)}"

    prompt = f"""你是7L街舞工作室的AI助手。根据用户输入，识别意图并提取参数。

7L系统支持以下意图：
{INTENT_DESCRIPTIONS}
{card_list_desc}
{history_str}
{pending_str}

用户输入：{message}

请返回JSON（不要其他文字）：
{{
  "intent": "意图类型",
  "params": {{参数键值对}},
  "reply": "给用户的回复（自然语言，简洁友好）",
  "need_confirm": true/false,
  "missing_params": ["缺少的必要参数名"]
}}

规则：
1. 如果参数完整且意图明确，need_confirm=true，reply写确认语
2. 如果缺少必要参数，need_confirm=false，reply追问缺少的参数
3. 金额统一为数字（不含¥符号）
4. 姓名统一为两个字以上的中文名
5. 卡种名称尽量匹配卡种列表中的名称，但如果用户说的卡种不在列表中，直接使用用户说的名称
6. 课次和金额允许为0，但不能同时为0
6. 如果用户说"确认"/"对"/"是的"等，intent填"confirm"
7. 如果用户说"取消"/"不要了"等，intent填"cancel"
8. 手机号必须脱敏为138****1234格式再返回
9. 回复要简洁，一句话说清楚"""

    result = await call_llm([{"role": "user", "content": prompt}])

    if not result:
        # 降级：简单规则匹配
        return _fallback_intent(message, session)

    try:
        # 提取JSON
        match = re.search(r'\{.*\}', result, re.DOTALL)
        if match:
            parsed = json.loads(match.group())
            # 脱敏手机号
            if parsed.get("params"):
                parsed["params"] = desensitize_data(parsed["params"])
            return parsed
    except json.JSONDecodeError:
        pass

    # JSON解析失败，降级
    return _fallback_intent(message, session)


def _fallback_intent(message: str, session: Optional[dict] = None) -> dict:
    """规则降级：简单关键词匹配"""
    msg = message.strip()

    # 确认/取消
    if msg in ("确认", "对", "是的", "好的", "确认执行", "执行"):
        return {"intent": "confirm", "params": {}, "reply": "", "need_confirm": False, "missing_params": []}
    if msg in ("取消", "不要了", "算了", "放弃"):
        return {"intent": "cancel", "params": {}, "reply": "好的，已取消操作。", "need_confirm": False, "missing_params": []}

    # 录入
    if any(kw in msg for kw in ["录入", "新增", "注册", "买卡", "办卡", "新学员"]):
        name = _extract_name(msg)
        params = {"姓名": name} if name else {}
        missing = [] if name else ["姓名"]
        reply = f"确认录入学员「{name}」？" if name else "请告诉我学员姓名"
        return {"intent": "register", "params": params, "reply": reply, "need_confirm": bool(name), "missing_params": missing}

    # 扣课
    if any(kw in msg for kw in ["扣课", "签到", "上课", "打卡"]):
        name = _extract_name(msg)
        params = {"姓名": name} if name else {}
        missing = [] if name else ["姓名"]
        reply = f"确认扣课签到「{name}」？" if name else "请告诉我学员姓名"
        return {"intent": "deduct", "params": params, "reply": reply, "need_confirm": bool(name), "missing_params": missing}

    # 查询
    if any(kw in msg for kw in ["查询", "查看", "搜索", "找", "信息"]):
        name = _extract_name(msg)
        params = {"姓名": name} if name else {}
        missing = [] if name else ["姓名"]
        reply = f"正在查询「{name}」..." if name else "请告诉我学员姓名"
        return {"intent": "query", "params": params, "reply": reply, "need_confirm": False, "missing_params": missing}

    # 统计
    if any(kw in msg for kw in ["统计", "数据", "营收", "收入"]):
        return {"intent": "stats", "params": {}, "reply": "正在获取统计数据...", "need_confirm": False, "missing_params": []}

    # 续费
    if any(kw in msg for kw in ["续费", "充值", "续卡"]):
        name = _extract_name(msg)
        params = {"姓名": name} if name else {}
        missing = [] if name else ["姓名"]
        reply = f"请选择续费的卡种" if name else "请告诉我学员姓名"
        return {"intent": "renew", "params": params, "reply": reply, "need_confirm": False, "missing_params": missing}

    # 退卡
    if any(kw in msg for kw in ["退卡", "退款", "退费"]):
        name = _extract_name(msg)
        params = {"姓名": name} if name else {}
        missing = [] if name else ["姓名"]
        reply = f"确认退卡「{name}」？" if name else "请告诉我学员姓名"
        return {"intent": "refund", "params": params, "reply": reply, "need_confirm": bool(name), "missing_params": missing}

    # 撤销
    if any(kw in msg for kw in ["撤销", "撤回", "回退"]):
        name = _extract_name(msg)
        params = {"姓名": name} if name else {}
        return {"intent": "undo", "params": params, "reply": "正在撤销最近操作...", "need_confirm": False, "missing_params": []}

    # 默认
    return {
        "intent": "unknown",
        "params": {},
        "reply": "抱歉，我没理解你的意思。你可以试试：\n• 录入学员（如：张三买了次卡16次，980元）\n• 扣课签到（如：张三签到）\n• 查询学员（如：查张三）\n• 查看统计",
        "need_confirm": False,
        "missing_params": [],
    }


def _extract_name(msg: str) -> str:
    """从消息中提取姓名（简单规则）"""
    # 常见模式：XX买了、XX签到、查XX、XX的
    patterns = [
        r'^([^\s,，、买办查签扣退续搜找]{2,4})[买办查签扣退续搜找]',
        r'[查找看]([^\s,，、的]{2,4})',
        r'^([^\s,，、]{2,4})[的买办签扣退续]',
    ]
    for p in patterns:
        m = re.search(p, msg)
        if m:
            return m.group(1)
    return ""


# ── AI对话主流程 ──────────────────────────────────────

async def ai_chat(message: str, session_id: Optional[str] = None) -> dict:
    """
    AI对话主入口。
    返回: {
        "session_id": str,
        "reply": str,
        "state": str,  # idle, collecting, confirming
        "pending_action": dict | None,
        "need_confirm": bool,
    }
    """
    # 获取或创建session
    if session_id:
        session = session_manager.get_session(session_id)
        if not session:
            # session过期，创建新的
            session_id = session_manager.create_session()
            session = session_manager.get_session(session_id)
    else:
        session_id = session_manager.create_session()
        session = session_manager.get_session(session_id)

    # 记录用户消息
    session["history"].append({"role": "user", "content": message})

    # 加载卡种定价
    card_prices = await _get_card_prices()

    # 意图识别
    intent_result = await recognize_intent(message, session, card_prices)

    intent = intent_result.get("intent", "unknown")
    params = intent_result.get("params", {})
    reply = intent_result.get("reply", "")
    need_confirm = intent_result.get("need_confirm", False)
    missing_params = intent_result.get("missing_params", [])

    # 处理确认/取消
    if intent == "confirm" and session.get("pending_action"):
        # 用户确认执行
        pending = session["pending_action"]
        result = await execute_action(pending["type"], pending["params"])
        session["pending_action"] = None
        session["state"] = "idle"
        session["history"].append({"role": "ai", "content": result.get("message", "执行完成")})
        return {
            "session_id": session_id,
            "reply": result.get("message", "执行完成"),
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    if intent == "cancel":
        session["pending_action"] = None
        session["state"] = "idle"
        session["history"].append({"role": "ai", "content": reply or "好的，已取消操作。"})
        return {
            "session_id": session_id,
            "reply": reply or "好的，已取消操作。",
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    # 补充参数：如果有pending_action且新消息提供了参数
    if session.get("pending_action") and intent not in ("confirm", "cancel"):
        pending = session["pending_action"]
        # 合并参数
        merged_params = {**pending["params"], **params}
        pending["params"] = merged_params
        params = merged_params
        intent = pending["type"]

    # 检查是否缺少参数
    if missing_params:
        session["state"] = "collecting"
        session["pending_action"] = {"type": intent, "params": params}
        session["history"].append({"role": "ai", "content": reply})
        return {
            "session_id": session_id,
            "reply": reply,
            "state": "collecting",
            "pending_action": {"type": intent, "params": params},
            "need_confirm": False,
        }

    # 参数完整，需要确认
    if need_confirm:
        session["state"] = "confirming"
        session["pending_action"] = {"type": intent, "params": params}
        session["history"].append({"role": "ai", "content": reply})
        return {
            "session_id": session_id,
            "reply": reply,
            "state": "confirming",
            "pending_action": {"type": intent, "params": params},
            "need_confirm": True,
        }

    # 不需要确认的操作（查询、统计等），直接执行
    if intent in ("query", "stats", "undo"):
        result = await execute_action(intent, params)
        session["state"] = "idle"
        session["pending_action"] = None
        session["history"].append({"role": "ai", "content": result.get("message", "执行完成")})
        return {
            "session_id": session_id,
            "reply": result.get("message", "执行完成"),
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    # 其他情况
    session["history"].append({"role": "ai", "content": reply})
    return {
        "session_id": session_id,
        "reply": reply,
        "state": "idle",
        "pending_action": None,
        "need_confirm": False,
    }


# ── 确认执行 ──────────────────────────────────────────

async def ai_confirm(session_id: str, confirmed: bool) -> dict:
    """确认或取消执行"""
    session = session_manager.get_session(session_id)
    if not session:
        return {
            "session_id": session_id,
            "reply": "操作已超时，如需继续请重新输入",
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    if not confirmed:
        session["pending_action"] = None
        session["state"] = "idle"
        session["history"].append({"role": "ai", "content": "好的，已取消操作。"})
        return {
            "session_id": session_id,
            "reply": "好的，已取消操作。",
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    pending = session.get("pending_action")
    if not pending:
        return {
            "session_id": session_id,
            "reply": "没有待执行的操作",
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    # 频率限制
    if not rate_limiter.check(pending["type"]):
        return {
            "session_id": session_id,
            "reply": "操作太频繁，请稍后再试",
            "state": "idle",
            "pending_action": None,
            "need_confirm": False,
        }

    # 执行
    result = await execute_action(pending["type"], pending["params"])
    session["pending_action"] = None
    session["state"] = "idle"
    session["history"].append({"role": "ai", "content": result.get("message", "执行完成")})

    return {
        "session_id": session_id,
        "reply": result.get("message", "执行完成"),
        "state": "idle",
        "pending_action": None,
        "need_confirm": False,
    }


# ── 执行操作 ──────────────────────────────────────────

async def execute_action(intent: str, params: dict) -> dict:
    """根据意图类型执行对应操作，返回结果"""
    try:
        if intent == "register":
            return await _execute_register(params)
        elif intent == "deduct":
            return await _execute_deduct(params)
        elif intent == "query":
            return await _execute_query(params)
        elif intent == "stats":
            return await _execute_stats(params)
        elif intent == "renew":
            return await _execute_renew(params)
        elif intent == "refund":
            return await _execute_refund(params)
        elif intent == "update":
            return await _execute_update(params)
        elif intent == "freeze":
            return await _execute_freeze(params)
        elif intent == "undo":
            return await _execute_undo(params)
        elif intent == "multi":
            return await _execute_multi(params)
        else:
            return {"message": "未知的操作类型", "success": False}
    except Exception as e:
        logger.error(f"执行操作失败: intent={intent}, params={params}, error={e}", exc_info=True)
        return {"message": f"操作失败：{str(e)}", "success": False}


async def _execute_register(params: dict) -> dict:
    """录入学员"""
    from app.routers.api import register_student, RegisterRequest

    name = params.get("姓名", "")
    if not name:
        return {"message": "姓名不能为空", "success": False}

    # 课次和金额：允许为0但不能同时为0
    hours = params.get("课次") or params.get("课时") or params.get("总课时")
    amount = params.get("金额")
    
    hours_val = float(hours) if hours else 0
    amount_val = float(amount) if amount else 0
    
    if hours_val == 0 and amount_val == 0:
        return {"message": "课次和金额不能同时为0，请至少填写一项", "success": False}

    req = RegisterRequest(
        name=name,
        card_name=params.get("卡种名称", ""),
        hours=hours_val if hours else None,
        amount=amount_val if amount else None,
        payment_method=params.get("付款方式", "微信"),
        phone=params.get("电话", ""),
        wechat=params.get("微信昵称", ""),
        channel=params.get("渠道来源", ""),
        note=params.get("备注", ""),
    )

    result = await register_student(req)
    
    # 构建回复：如果有卡种名称就显示，没有就不显示
    card_info = f"{result.get('card_name', '')}，" if result.get('card_name') else ""
    hours_info = f"{result.get('total_hours', 0)}次课，" if result.get('total_hours', 0) else ""
    return {
        "message": f"✅ 录入成功！{name}，{card_info}{hours_info}金额¥{result.get('amount', 0)}，有效期至{result.get('expire_date', '')}",
        "success": True,
        "data": result,
    }


async def _execute_deduct(params: dict) -> dict:
    """扣课签到"""
    from app.routers.api import deduct_class, DeductRequest

    name = params.get("姓名", "")
    if not name:
        return {"message": "姓名不能为空", "success": False}

    req = DeductRequest(
        name=name,
        deduct_count=int(params.get("扣课次数", 1)),
        teacher=params.get("老师", ""),
        dance_type=params.get("舞种", ""),
    )

    result = await deduct_class(req)
    msg = f"✅ 扣课成功！{name}，扣{result.get('deducted', 1)}次，剩余{result.get('remaining', 0)}次"
    if result.get("warning"):
        msg += f"\n⚠️ {result['warning']}"
    return {"message": msg, "success": True, "data": result}


async def _execute_query(params: dict) -> dict:
    """查询学员"""
    from app.routers.api import get_student

    name = params.get("姓名", "")
    if not name:
        return {"message": "请告诉我学员姓名", "success": False}

    try:
        s = await get_student(name)
    except Exception as e:
        return {"message": f"未找到学员「{name}」", "success": False}

    status_emoji = "🟢" if s.get("card_status") == "有效" else "🔴"
    remaining = "不限次" if s.get("card_type") == "月卡" else f"{s.get('remaining_hours', 0)}次"

    msg = f"""📋 **{s.get('name', name)}** {status_emoji}
卡种：{s.get('card_name', '-')}
卡类型：{s.get('card_type', '-')}
状态：{s.get('card_status', '-')}
剩余：{remaining}
金额：¥{s.get('amount', 0)}
有效期至：{s.get('expire_date', '-')}
电话：{desensitize_phone(s.get('phone', ''))}
最近上课：{len(s.get('class_records', []))}次"""

    return {"message": msg, "success": True, "data": s}


async def _execute_stats(params: dict) -> dict:
    """统计数据"""
    from app.routers.api import get_stats

    stats = await get_stats()

    msg = f"""📊 **本月统计**
💰 月营收：¥{stats.get('month_revenue', 0):,.0f}
📅 消课数：{stats.get('month_class_count', 0)}
👤 新学员：{stats.get('month_new_students', 0)}
👥 有效学员：{stats.get('total_active', 0)}
🔄 续费率：{stats.get('renew_rate', 0)}%"""

    if stats.get("expiring_soon"):
        msg += f"\n⚠️ 即将过期：{len(stats['expiring_soon'])}人"
    if stats.get("low_hours"):
        msg += f"\n🔋 课时不足：{len(stats['low_hours'])}人"

    return {"message": msg, "success": True, "data": stats}


async def _execute_renew(params: dict) -> dict:
    """续费"""
    from app.routers.api import renew_student, RenewRequest

    name = params.get("姓名", "")
    card_name = params.get("卡种名称", "")
    if not name:
        return {"message": "请告诉我学员姓名", "success": False}
    if not card_name:
        return {"message": "请选择续费的卡种", "success": False}

    req = RenewRequest(
        name=name,
        card_name=card_name,
        amount=float(params["金额"]) if params.get("金额") else None,
        payment_method=params.get("付款方式", "微信"),
    )

    result = await renew_student(req)
    return {
        "message": f"✅ 续费成功！{name}，{card_name}，剩余{result.get('new_remaining', 0)}次，有效期至{result.get('expire_date', '')}",
        "success": True,
        "data": result,
    }


async def _execute_refund(params: dict) -> dict:
    """退卡"""
    from app.routers.api import refund_student

    name = params.get("姓名", "")
    if not name:
        return {"message": "请告诉我学员姓名", "success": False}

    result = await refund_student(name)
    return {"message": f"✅ 退卡成功！{name}", "success": True, "data": result}


async def _execute_update(params: dict) -> dict:
    """修改信息"""
    from app.routers.api import update_student
    from app.services.bitable_client import bitable
    from app.config import settings

    name = params.get("姓名", "")
    if not name:
        return {"message": "请告诉我学员姓名", "success": False}

    # 找到学员记录
    records = await bitable.find_member_by_name(name)
    if not records:
        return {"message": f"未找到学员「{name}」", "success": False}

    active = [r for r in records if r.get("fields", {}).get("卡状态") == "有效"]
    record = active[0] if active else records[0]
    record_id = record.get("record_id")

    fields = {}
    if params.get("电话"):
        fields["电话"] = params["电话"]
    if params.get("微信昵称"):
        fields["微信昵称"] = params["微信昵称"]
    if params.get("备注"):
        fields["备注"] = params["备注"]

    if not fields:
        return {"message": "没有需要更新的字段", "success": False}

    await bitable.update_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        record_id,
        fields,
    )

    return {"message": f"✅ 修改成功！{name}，已更新：{', '.join(fields.keys())}", "success": True}


async def _execute_freeze(params: dict) -> dict:
    """请假/冻结"""
    from app.services.bitable_client import bitable
    from app.config import settings

    name = params.get("姓名", "")
    if not name:
        return {"message": "请告诉我学员姓名", "success": False}

    records = await bitable.find_member_by_name(name)
    if not records:
        return {"message": f"未找到学员「{name}」", "success": False}

    active = [r for r in records if r.get("fields", {}).get("卡状态") == "有效"]
    if not active:
        return {"message": f"学员「{name}」没有有效卡", "success": False}

    record = active[0]
    record_id = record.get("record_id")
    fields = record.get("fields", {})

    # 简单实现：在备注中标记冻结
    days = int(params.get("天数", 7))
    note = fields.get("备注", "")
    freeze_note = f"[请假{days}天]"
    if note:
        new_note = f"{note} {freeze_note}"
    else:
        new_note = freeze_note

    await bitable.update_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        record_id,
        {"备注": new_note},
    )

    await bitable.add_log("店长", "请假", name, f"请假{days}天")

    return {"message": f"✅ 请假成功！{name}，已标记请假{days}天", "success": True}


async def _execute_undo(params: dict) -> dict:
    """撤销最近操作"""
    from app.services.bitable_client import bitable
    from app.config import settings

    name = params.get("姓名", "")

    # 获取最近操作日志
    logs = await bitable.get_recent_logs(student_name=name if name else None, limit=1)
    if not logs:
        return {"message": "没有可撤销的操作", "success": False}

    log = logs[0]
    f = log.get("fields", {})

    if f.get("是否已撤销"):
        return {"message": "最近操作已被撤销", "success": False}

    # 检查7天窗口
    op_time = f.get("操作时间", 0)
    if time.time() * 1000 - op_time > 7 * 24 * 60 * 60 * 1000:
        return {"message": "已超过7天撤销窗口期", "success": False}

    # 调用撤销API
    from app.routers.api import undo_operation
    result = await undo_operation(log["record_id"])
    return {"message": f"✅ {result.get('message', '撤销成功')}", "success": True}


async def _execute_multi(params: dict) -> dict:
    """多意图组合执行"""
    actions = params.get("actions", [])
    if not actions:
        return {"message": "未识别到具体操作", "success": False}

    results = []
    for action in actions:
        intent = action.get("type", "")
        action_params = action.get("params", {})
        result = await execute_action(intent, action_params)
        results.append(result)

    success_count = sum(1 for r in results if r.get("success"))
    fail_count = len(results) - success_count

    msg = f"执行完成：成功{success_count}个"
    if fail_count > 0:
        msg += f"，失败{fail_count}个"
    msg += "\n" + "\n".join(r.get("message", "") for r in results)

    return {"message": msg, "success": fail_count == 0}


# ── 辅助函数 ──────────────────────────────────────────

async def _get_card_prices() -> list[dict]:
    """获取卡种定价列表（带缓存）"""
    global _card_prices_cache
    now = time.time()
    if now - _card_prices_cache["ts"] > 300:  # 5分钟缓存
        try:
            from app.services.bitable_client import bitable
            from app.config import settings
            records = await bitable.get_pricing_list()
            prices = []
            for r in records:
                f = r.get("fields", {})
                name = f.get("卡种名称", "")
                if name:
                    prices.append({
                        "name": name,
                        "card_type": f.get("卡类型", ""),
                        "price": float(f.get("金额", 0)),
                        "hours": int(float(f.get("课时数", 0))),
                        "valid_days": int(float(f.get("有效期天", 30))),
                    })
            _card_prices_cache = {"data": prices, "ts": now}
        except Exception as e:
            logger.error(f"获取卡种定价失败: {e}")

    return _card_prices_cache["data"]


# 启动时初始化session清理
def init_ai_service():
    """初始化AI服务（在app启动时调用）"""
    session_manager.start_cleanup()
    logger.info("AI service initialized")
