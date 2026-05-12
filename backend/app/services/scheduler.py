"""定时任务 - 提醒服务"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from app.services.bitable_client import bitable
from app.services.feishu_client import feishu_client
from app.services.command_handler import _fmt_date, _days_later_ms, _ts
from app.config import settings

logger = logging.getLogger(__name__)
scheduler = AsyncIOScheduler()

# 默认提醒群（启动后通过环境变量或配置设置）
REMIND_CHAT_ID = ""


async def check_expiring_cards():
    """检查即将过期的卡，发送提醒"""
    if not REMIND_CHAT_ID:
        logger.warning("未配置提醒群ID，跳过过期提醒")
        return

    try:
        active = await bitable.get_active_members()
        expiring_7d = []
        expiring_3d = []

        for r in active:
            f = r.get("fields", {})
            expire = f.get("有效期至", 0)
            if not expire:
                continue
            name = f.get("姓名", "?")
            card = f.get("卡种名称", "?")
            remaining = f.get("剩余课时", 0)

            if expire < _days_later_ms(3):
                expiring_3d.append(f"  {name} | {card} | 剩{remaining}次 | {_fmt_date(expire)}")
            elif expire < _days_later_ms(7):
                expiring_7d.append(f"  {name} | {card} | 剩{remaining}次 | {_fmt_date(expire)}")

        if expiring_3d or expiring_7d:
            lines = ["⏰ 卡片过期提醒", ""]
            if expiring_3d:
                lines.append("🔴 3天内过期：")
                lines.extend(expiring_3d)
                lines.append("")
            if expiring_7d:
                lines.append("🟡 7天内过期：")
                lines.extend(expiring_7d)

            await feishu_client.send_text(REMIND_CHAT_ID, "\n".join(lines))

    except Exception as e:
        logger.error(f"过期提醒失败: {e}")


async def check_low_credits():
    """检查课时不足的学员"""
    if not REMIND_CHAT_ID:
        return

    try:
        active = await bitable.get_active_members()
        low = []

        for r in active:
            f = r.get("fields", {})
            remaining = float(f.get("剩余课时", 999))
            if remaining <= 2:
                name = f.get("姓名", "?")
                card = f.get("卡种名称", "?")
                low.append(f"  {name} | {card} | 剩{remaining}次")

        if low:
            lines = ["⚠️ 课时不足提醒", ""]
            lines.extend(low)
            await feishu_client.send_text(REMIND_CHAT_ID, "\n".join(lines))

    except Exception as e:
        logger.error(f"课时提醒失败: {e}")


def setup_scheduler(chat_id: str = ""):
    """配置定时任务"""
    global REMIND_CHAT_ID
    if chat_id:
        REMIND_CHAT_ID = chat_id

    # 每天早上9点检查过期
    scheduler.add_job(check_expiring_cards, "cron", hour=9, minute=0, id="expiring_check")

    # 每天早上9点检查课时不足
    scheduler.add_job(check_low_credits, "cron", hour=9, minute=5, id="low_credits_check")

    logger.info("定时任务已配置: 9:00过期提醒, 9:05课时提醒")
