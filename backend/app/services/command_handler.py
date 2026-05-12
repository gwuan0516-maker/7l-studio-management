"""命令处理器 - 执行解析后的命令"""
import time
from datetime import datetime, timedelta
from app.services.bitable_client import bitable
from app.services.feishu_client import feishu_client
from app.services.ocr_service import ocr_service
from app.services.command_parser import ParsedCommand, CommandType, format_help
from app.config import settings


def _ts() -> int:
    """当前时间戳（毫秒）"""
    return int(time.time() * 1000)


def _today_ms() -> int:
    """今天0点的时间戳（毫秒）"""
    now = datetime.now()
    return int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)


def _days_later_ms(days: int) -> int:
    """N天后的时间戳（毫秒）"""
    future = datetime.now() + timedelta(days=days)
    return int(future.replace(hour=23, minute=59, second=59).timestamp() * 1000)


def _fmt_date(ts_ms) -> str:
    """格式化日期"""
    if not ts_ms:
        return "无"
    return datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d")


async def handle_command(cmd: ParsedCommand, operator: str = "店长", chat_id: str = "") -> str:
    """处理命令，返回回复文本"""
    try:
        if cmd.cmd == CommandType.HELP:
            return format_help()

        elif cmd.cmd == CommandType.QUERY:
            return await _handle_query(cmd)

        elif cmd.cmd == CommandType.REGISTER:
            return await _handle_register(cmd, operator)

        elif cmd.cmd == CommandType.DEDUCT:
            return await _handle_deduct(cmd, operator)

        elif cmd.cmd == CommandType.RENEW:
            return await _handle_renew(cmd, operator)

        elif cmd.cmd == CommandType.REFUND:
            return await _handle_refund(cmd, operator)

        elif cmd.cmd == CommandType.MODIFY:
            return await _handle_modify(cmd, operator)

        elif cmd.cmd == CommandType.UNDO:
            return await _handle_undo(cmd, operator)

        elif cmd.cmd == CommandType.STATS:
            return await _handle_stats(cmd)

        elif cmd.cmd == CommandType.LIST:
            return await _handle_list(cmd)

        elif cmd.cmd == CommandType.REMIND:
            return await _handle_remind(cmd, chat_id)

        else:
            return f"❓ 未知命令: {cmd.raw}\n输入「帮助」查看可用命令"

    except Exception as e:
        return f"⚠️ 处理出错: {str(e)}\n请检查输入格式，或联系管理员"


# ── 查询 ──────────────────────────────────────────────

async def _handle_query(cmd: ParsedCommand) -> str:
    """查询学员信息"""
    if cmd.member_id:
        records = await bitable.find_member_by_member_id(cmd.member_id)
    elif cmd.name:
        records = await bitable.find_member_by_name(cmd.name)
    else:
        return "❌ 请输入查询关键词，如：查询 张三"

    if not records:
        return f"🔍 未找到学员「{cmd.name or cmd.member_id}」"

    lines = []
    for r in records:
        f = r.get("fields", {})
        lines.append(f"📋 学员信息")
        lines.append(f"  姓名：{f.get('姓名', '-')}")
        lines.append(f"  会员号：{f.get('会员号', '-')}")
        lines.append(f"  微信昵称：{f.get('微信昵称', '-')}")
        lines.append(f"  卡类型：{f.get('卡类型', '-')}")
        lines.append(f"  卡种名称：{f.get('卡种名称', '-')}")
        lines.append(f"  金额：{f.get('金额', '-')}")
        lines.append(f"  总课时：{f.get('总课时', '-')}")
        lines.append(f"  剩余课时：{f.get('剩余课时', '-')}")
        lines.append(f"  卡状态：{f.get('卡状态', '-')}")
        lines.append(f"  激活日期：{_fmt_date(f.get('激活日期'))}")
        lines.append(f"  有效期至：{_fmt_date(f.get('有效期至'))}")
        lines.append(f"  电话：{f.get('电话', '-')}")
        lines.append(f"  备注：{f.get('备注', '-')}")
        lines.append("─" * 20)

    return "\n".join(lines)


# ── 录入 ──────────────────────────────────────────────

async def _handle_register(cmd: ParsedCommand, operator: str) -> str:
    """录入新学员"""
    if not cmd.name:
        return "❌ 请输入学员姓名，如：录入 张三 次卡·8次 微信 580"

    # 检查是否已存在
    existing = await bitable.find_member_by_name(cmd.name)
    if existing:
        active = [r for r in existing if r.get("fields", {}).get("卡状态") == "有效"]
        if active:
            return f"⚠️ 学员「{cmd.name}」已有有效卡，请先退卡或续费"

    # 查找卡种定价
    card_info = await _resolve_card_info(cmd.card_name)
    if not card_info and cmd.card_name:
        return f"❌ 未找到卡种「{cmd.card_name}」，请检查卡种名称"

    # 构建记录
    fields = {
        "7L街舞工作室管理系统": cmd.name,
        "姓名": cmd.name,
    }

    if card_info:
        fields["卡类型"] = card_info.get("卡类型", "")
        fields["卡种名称"] = card_info.get("卡种名称", cmd.card_name or "")
        fields["总课时"] = float(card_info.get("课时数", 0))
        fields["剩余课时"] = float(card_info.get("课时数", 0))
        fields["金额"] = float(card_info.get("金额", 0))
        valid_days = int(float(card_info.get("有效期天", 30)))
        fields["激活日期"] = _ts()
        fields["有效期至"] = _days_later_ms(valid_days)
    else:
        # 没有匹配卡种，用默认值
        fields["卡类型"] = cmd.card_type or "次卡"
        fields["卡种名称"] = cmd.card_name or ""
        fields["总课时"] = 0
        fields["剩余课时"] = 0
        fields["金额"] = cmd.amount or 0
        fields["激活日期"] = _ts()
        fields["有效期至"] = _days_later_ms(30)

    if cmd.payment_method:
        fields["付款方式"] = cmd.payment_method
    if cmd.amount and card_info:
        # 用户指定金额覆盖
        fields["金额"] = cmd.amount
    elif cmd.amount:
        fields["金额"] = cmd.amount
    if cmd.note:
        fields["备注"] = cmd.note

    fields["付款日期"] = _ts()
    fields["卡状态"] = "有效"

    # 生成会员号
    member_id = f"7L{int(time.time()) % 1000000:06d}"
    fields["会员号"] = member_id

    record = await bitable.create_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        fields,
    )

    # 写操作日志
    detail = f"录入卡种:{fields.get('卡种名称','')} 金额:{fields.get('金额',0)} 课时:{fields.get('总课时',0)}"
    await bitable.add_log(operator, "录入", cmd.name, detail)

    return (
        f"✅ 录入成功！\n"
        f"  姓名：{cmd.name}\n"
        f"  会员号：{member_id}\n"
        f"  卡种：{fields.get('卡种名称', '-')}\n"
        f"  课时：{fields.get('总课时', 0)}\n"
        f"  金额：{fields.get('金额', 0)}\n"
        f"  有效期至：{_fmt_date(fields.get('有效期至'))}"
    )


async def _resolve_card_info(card_name: str) -> dict | None:
    """从卡种定价表查找卡种信息"""
    if not card_name:
        return None
    pricing_records = await bitable.get_pricing_list()
    for r in pricing_records:
        f = r.get("fields", {})
        if f.get("卡种名称") == card_name:
            return f
    # 模糊匹配
    for r in pricing_records:
        f = r.get("fields", {})
        name = f.get("卡种名称", "")
        if name and card_name in name:
            return f
    return None


# ── 扣课 ──────────────────────────────────────────────

async def _handle_deduct(cmd: ParsedCommand, operator: str) -> str:
    """扣课签到"""
    if not cmd.name:
        return "❌ 请输入学员姓名，如：扣课 张三"

    records = await bitable.find_member_by_name(cmd.name)
    if not records:
        return f"❌ 未找到学员「{cmd.name}」"

    # 找有效卡
    active = [r for r in records if r.get("fields", {}).get("卡状态") == "有效"]
    if not active:
        return f"❌ 学员「{cmd.name}」没有有效卡"

    record = active[0]
    fields = record.get("fields", {})
    record_id = record.get("record_id")

    remaining = float(fields.get("剩余课时", 0))
    deduct = cmd.deduct_count or 1

    # 月卡不扣课时
    if fields.get("卡类型") == "月卡":
        # 检查有效期
        expire = fields.get("有效期至", 0)
        if expire and expire < _ts():
            return f"❌ 学员「{cmd.name}」月卡已过期"

        # 记录上课但不扣课时
        await bitable.add_class_record({
            "7L-上课登记": f"{cmd.name}-{_fmt_date(_ts())}",
            "学员姓名": cmd.name,
            "上课日期": _ts(),
            "老师": cmd.teacher or "",
            "舞种": cmd.dance_type or "",
            "扣课数": 0,
            "扣课前剩余": remaining,
            "扣课后剩余": remaining,
            "关联卡号": fields.get("会员号", ""),
        })
        await bitable.add_log(operator, "扣课", cmd.name,
                              f"月卡签到 老师:{cmd.teacher or '-'} 舞种:{cmd.dance_type or '-'}")
        return f"✅ 月卡签到成功！\n  学员：{cmd.name}\n  卡种：月卡（不限次）"

    # 次卡/期卡/体验卡 - 扣课时
    if remaining < deduct:
        return f"❌ 课时不足！剩余 {remaining} 次，需要扣 {deduct} 次"

    new_remaining = remaining - deduct
    await bitable.update_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        record_id,
        {"剩余课时": new_remaining},
    )

    # 上课登记
    await bitable.add_class_record({
        "7L-上课登记": f"{cmd.name}-{_fmt_date(_ts())}",
        "学员姓名": cmd.name,
        "上课日期": _ts(),
        "老师": cmd.teacher or "",
        "舞种": cmd.dance_type or "",
        "扣课数": deduct,
        "扣课前剩余": remaining,
        "扣课后剩余": new_remaining,
        "关联卡号": fields.get("会员号", ""),
    })

    # 日志
    detail = f"扣{deduct}次 课前:{remaining} 课后:{new_remaining} 老师:{cmd.teacher or '-'}"
    await bitable.add_log(operator, "扣课", cmd.name, detail)

    # 课时不足提醒
    warning = ""
    if new_remaining <= 2:
        warning = "\n⚠️ 课时即将用完，请提醒续费！"

    return (
        f"✅ 扣课成功！\n"
        f"  学员：{cmd.name}\n"
        f"  扣除：{deduct} 次\n"
        f"  剩余：{new_remaining} 次{warning}"
    )


# ── 续费 ──────────────────────────────────────────────

async def _handle_renew(cmd: ParsedCommand, operator: str) -> str:
    """续费"""
    if not cmd.name:
        return "❌ 请输入学员姓名，如：续费 张三 次卡·16次 微信 980"

    records = await bitable.find_member_by_name(cmd.name)
    if not records:
        return f"❌ 未找到学员「{cmd.name}」"

    # 找有效卡（优先）或最近卡
    active = [r for r in records if r.get("fields", {}).get("卡状态") == "有效"]
    record = active[0] if active else records[0]
    fields = record.get("fields", {})
    record_id = record.get("record_id")

    # 查找新卡种
    card_info = await _resolve_card_info(cmd.card_name)
    if not card_info:
        return f"❌ 未找到卡种「{cmd.card_name}」"

    old_remaining = float(fields.get("剩余课时", 0))
    new_total = float(card_info.get("课时数", 0))
    new_remaining = old_remaining + new_total
    valid_days = int(float(card_info.get("有效期天", 30)))

    update_fields = {
        "卡类型": card_info.get("卡类型", fields.get("卡类型", "")),
        "卡种名称": card_info.get("卡种名称", cmd.card_name or ""),
        "总课时": float(fields.get("总课时", 0)) + new_total,
        "剩余课时": new_remaining,
        "金额": float(fields.get("金额", 0)) + (cmd.amount or float(card_info.get("金额", 0))),
        "有效期至": _days_later_ms(valid_days),
        "卡状态": "有效",
    }
    if cmd.payment_method:
        update_fields["付款方式"] = cmd.payment_method

    await bitable.update_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        record_id,
        update_fields,
    )

    detail = f"续费:{card_info.get('卡种名称','')} +{new_total}课时 金额:{cmd.amount or card_info.get('金额',0)}"
    await bitable.add_log(operator, "续费", cmd.name, detail)

    return (
        f"✅ 续费成功！\n"
        f"  学员：{cmd.name}\n"
        f"  新增课时：{new_total} 次\n"
        f"  当前剩余：{new_remaining} 次\n"
        f"  新有效期至：{_fmt_date(update_fields['有效期至'])}"
    )


# ── 退卡 ──────────────────────────────────────────────

async def _handle_refund(cmd: ParsedCommand, operator: str) -> str:
    """退卡"""
    if not cmd.name:
        return "❌ 请输入学员姓名，如：退卡 张三"

    records = await bitable.find_member_by_name(cmd.name)
    if not records:
        return f"❌ 未找到学员「{cmd.name}」"

    active = [r for r in records if r.get("fields", {}).get("卡状态") == "有效"]
    if not active:
        return f"❌ 学员「{cmd.name}」没有有效卡"

    record = active[0]
    fields = record.get("fields", {})
    record_id = record.get("record_id")

    refund_amount = cmd.amount or 0
    update_fields = {
        "卡状态": "已退卡",
        "退款金额": refund_amount,
    }

    await bitable.update_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        record_id,
        update_fields,
    )

    detail = f"退卡 剩余课时:{fields.get('剩余课时',0)} 退款:{refund_amount}"
    await bitable.add_log(operator, "退卡", cmd.name, detail)

    return (
        f"✅ 退卡成功！\n"
        f"  学员：{cmd.name}\n"
        f"  退卡前剩余：{fields.get('剩余课时', 0)} 次\n"
        f"  退款金额：{refund_amount}"
    )


# ── 修改 ──────────────────────────────────────────────

async def _handle_modify(cmd: ParsedCommand, operator: str) -> str:
    """修改学员信息"""
    if not cmd.name or not cmd.field_name or not cmd.field_value:
        return "❌ 格式：修改 张三 电话 13800138000"

    records = await bitable.find_member_by_name(cmd.name)
    if not records:
        return f"❌ 未找到学员「{cmd.name}」"

    record = records[0]
    record_id = record.get("record_id")
    old_value = record.get("fields", {}).get(cmd.field_name, "-")

    # 字段类型映射
    field_value = cmd.field_value
    if cmd.field_name in ("金额", "总课时", "剩余课时", "退款金额"):
        try:
            field_value = float(cmd.field_value)
        except ValueError:
            return f"❌ {cmd.field_name} 应为数字"

    await bitable.update_record(
        settings.BITABLE_MAIN_APP_TOKEN,
        settings.BITABLE_MAIN_TABLE_ID,
        record_id,
        {cmd.field_name: field_value},
    )

    detail = f"修改 {cmd.field_name}: {old_value} → {field_value}"
    await bitable.add_log(operator, "修改", cmd.name, detail)

    return f"✅ 修改成功！\n  {cmd.field_name}: {old_value} → {field_value}"


# ── 撤销 ──────────────────────────────────────────────

async def _handle_undo(cmd: ParsedCommand, operator: str) -> str:
    """撤销最近操作"""
    target_name = cmd.name or cmd.log_id
    if not target_name:
        return "❌ 请指定学员姓名，如：撤销 张三"

    # 查找最近的未撤销日志
    logs = await bitable.get_recent_logs(student_name=target_name, limit=5)
    undoable = [l for l in logs if not l.get("fields", {}).get("是否已撤销")]
    if not undoable:
        return f"❌ 没有可撤销的操作"

    log = undoable[0]
    log_fields = log.get("fields", {})
    log_id = log.get("record_id")
    op_type = log_fields.get("操作类型", "")
    detail = log_fields.get("变更详情", "")

    # 根据操作类型执行反向操作
    if op_type == "扣课":
        # 反向：加回课时
        import re
        m = re.search(r'扣(\d+)次\s+课前:([\d.]+)\s+课后:([\d.]+)', detail)
        if m:
            deduct_count = int(m.group(1))
            pre_remaining = float(m.group(2))
            records = await bitable.find_member_by_name(target_name)
            if records:
                record = records[0]
                await bitable.update_record(
                    settings.BITABLE_MAIN_APP_TOKEN,
                    settings.BITABLE_MAIN_TABLE_ID,
                    record["record_id"],
                    {"剩余课时": pre_remaining},
                )

    elif op_type == "录入":
        # 反向：退卡
        records = await bitable.find_member_by_name(target_name)
        active = [r for r in records if r.get("fields", {}).get("卡状态") == "有效"]
        if active:
            await bitable.update_record(
                settings.BITABLE_MAIN_APP_TOKEN,
                settings.BITABLE_MAIN_TABLE_ID,
                active[0]["record_id"],
                {"卡状态": "已退卡"},
            )

    # 标记日志为已撤销
    await bitable.update_record(
        settings.BITABLE_LOG_APP_TOKEN,
        settings.BITABLE_LOG_TABLE_ID,
        log_id,
        {"是否已撤销": True},
    )

    await bitable.add_log(operator, "撤销", target_name, f"撤销操作: {op_type} - {detail}")

    return f"✅ 已撤销！\n  操作：{op_type}\n  详情：{detail}"


# ── 统计 ──────────────────────────────────────────────

async def _handle_stats(cmd: ParsedCommand) -> str:
    """统计数据"""
    # 获取所有有效卡
    active = await bitable.get_active_members()

    # 卡类型分布
    card_type_count = {}
    total_revenue = 0
    expiring_soon = []

    for r in active:
        f = r.get("fields", {})
        ct = f.get("卡类型", "未知")
        card_type_count[ct] = card_type_count.get(ct, 0) + 1
        total_revenue += float(f.get("金额", 0))

        # 即将过期（7天内）
        expire = f.get("有效期至", 0)
        if expire and expire < _days_later_ms(7):
            expiring_soon.append(f.get("姓名", ""))

    # 今日上课统计
    today_start = _today_ms()
    class_records = await bitable.list_records(
        settings.BITABLE_CLASS_APP_TOKEN,
        settings.BITABLE_CLASS_TABLE_ID,
        filter_expr=f'CurrentValue.[上课日期] >= {today_start}',
    )

    lines = [
        "📊 7L街舞工作室统计",
        "",
        f"👥 有效学员：{len(active)} 人",
    ]
    for ct, count in sorted(card_type_count.items()):
        lines.append(f"  {ct}：{count} 人")

    lines.append(f"💰 累计收入：¥{total_revenue:.0f}")
    lines.append(f"📅 今日上课：{len(class_records)} 人次")

    if expiring_soon:
        lines.append(f"⚠️ 即将过期：{', '.join(expiring_soon)}")

    return "\n".join(lines)


# ── 列表 ──────────────────────────────────────────────

async def _handle_list(cmd: ParsedCommand) -> str:
    """列表查询"""
    sub = cmd.extra.get("sub", "")

    if sub in ("卡种", "价格", "定价"):
        pricing = await bitable.get_pricing_list()
        lines = ["📋 卡种定价", ""]
        for r in pricing:
            f = r.get("fields", {})
            name = f.get("卡种名称", "")
            if not name:
                continue
            price = f.get("金额", 0)
            hours = f.get("课时数", 0)
            days = f.get("有效期天", 0)
            lines.append(f"  {name}  ¥{price}  {hours}次  {days}天")
        return "\n".join(lines)

    # 默认列出有效学员
    active = await bitable.get_active_members()
    if not active:
        return "📋 当前没有有效学员"

    lines = [f"📋 有效学员（{len(active)}人）", ""]
    for r in active:
        f = r.get("fields", {})
        name = f.get("姓名", "?")
        card = f.get("卡种名称", "?")
        remaining = f.get("剩余课时", 0)
        expire = _fmt_date(f.get("有效期至"))
        lines.append(f"  {name} | {card} | 剩{remaining}次 | 到{expire}")

    return "\n".join(lines)


# ── 提醒 ──────────────────────────────────────────────

async def _handle_remind(cmd: ParsedCommand, chat_id: str) -> str:
    """查看/设置提醒"""
    sub = cmd.extra.get("sub", "")

    if sub in ("过期", "即将过期"):
        # 查找7天内过期的卡
        active = await bitable.get_active_members()
        expiring = []
        for r in active:
            f = r.get("fields", {})
            expire = f.get("有效期至", 0)
            if expire and expire < _days_later_ms(7):
                expiring.append(f"{f.get('姓名', '?')} - 到{_fmt_date(expire)}")

        if not expiring:
            return "✅ 近7天没有即将过期的卡"
        return "⚠️ 即将过期：\n" + "\n".join(f"  {e}" for e in expiring)

    if sub in ("课时不足", "课时"):
        active = await bitable.get_active_members()
        low = []
        for r in active:
            f = r.get("fields", {})
            remaining = float(f.get("剩余课时", 999))
            if remaining <= 2:
                low.append(f"{f.get('姓名', '?')} - 剩{remaining}次")

        if not low:
            return "✅ 没有课时不足的学员"
        return "⚠️ 课时不足：\n" + "\n".join(f"  {l}" for l in low)

    return "提醒 子命令：\n  提醒 过期 - 查看即将过期的卡\n  提醒 课时 - 查看课时不足的学员"


# ── OCR截图处理 ────────────────────────────────────────

async def handle_ocr_image(image_key: str, operator: str = "店长") -> str:
    """处理接龙截图"""
    # 下载图片
    image_bytes = await feishu_client.download_image(image_key)

    # OCR识别
    lines = await ocr_service.recognize_image(image_bytes)
    if not lines:
        return "❌ 图片识别失败，未检测到文字"

    # 解析接龙
    students = ocr_service.parse_jielong_text(lines)
    if not students:
        return f"❌ 识别到文字但未解析到学员信息\n原始文本：\n" + "\n".join(lines[:10])

    # 批量录入/扣课
    results = []
    for s in students:
        name = s["name"]
        note = s["note"]

        # 检查是否已有有效卡
        existing = await bitable.find_member_by_name(name)
        active = [r for r in existing if r.get("fields", {}).get("卡状态") == "有效"]

        if active:
            # 已有卡 → 扣课
            cmd = ParsedCommand(
                cmd=CommandType.DEDUCT,
                raw=f"扣课 {name}",
                name=name,
                deduct_count=1,
                note=note,
            )
            result = await _handle_deduct(cmd, operator)
            results.append(f"📝 {name}: {result.split(chr(10))[0]}")
        else:
            # 没有卡 → 提示录入
            results.append(f"⚠️ {name}: 未找到有效卡，请先录入")

    summary = f"📸 截图识别结果（{len(students)}人）\n" + "\n".join(results)
    return summary
