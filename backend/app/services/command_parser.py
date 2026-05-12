"""命令解析器 - 解析飞书群消息为结构化命令"""
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class CommandType(Enum):
    # P0 - 基础
    QUERY = "查询"          # 查询学员信息
    HELP = "帮助"           # 帮助信息
    LIST = "列表"           # 列出学员/卡种

    # P1 - 核心业务
    REGISTER = "录入"       # 录入新学员
    DEDUCT = "扣课"         # 扣课签到
    OCR = "截图识别"        # OCR识别接龙截图

    # P2 - 高级
    STATS = "统计"          # 统计数据
    RENEW = "续费"          # 续费
    REFUND = "退卡"         # 退卡
    MODIFY = "修改"         # 修改信息
    UNDO = "撤销"           # 撤销操作
    REMIND = "提醒"         # 设置/查看提醒

    UNKNOWN = "未知"


@dataclass
class ParsedCommand:
    """解析后的命令"""
    cmd: CommandType
    raw: str
    name: Optional[str] = None          # 学员姓名
    member_id: Optional[str] = None     # 会员号
    card_type: Optional[str] = None     # 卡类型（次卡/月卡/期卡/体验卡）
    card_name: Optional[str] = None     # 卡种名称（如"次卡·8次"）
    amount: Optional[float] = None      # 金额
    payment_method: Optional[str] = None # 付款方式
    deduct_count: Optional[int] = None  # 扣课次数
    teacher: Optional[str] = None       # 老师
    dance_type: Optional[str] = None    # 舞种
    note: Optional[str] = None          # 备注
    field_name: Optional[str] = None    # 要修改的字段名
    field_value: Optional[str] = None   # 要修改的值
    log_id: Optional[str] = None        # 要撤销的日志ID
    extra: dict = field(default_factory=dict)


def parse_command(text: str) -> ParsedCommand:
    """
    解析群消息文本为结构化命令。
    
    命令格式：
    - 查询 张三
    - 查询 会员号001
    - 录入 张三 次卡·8次 微信 580
    - 录入 李四 体验卡 现金 49
    - 扣课 张三 1
    - 扣课 张三
    - 统计
    - 统计 本月
    - 续费 张三 次卡·16次 微信 980
    - 退卡 张三
    - 修改 张三 电话 13800138000
    - 撤销 日志ID
    - 列表
    - 列表 卡种
    - 帮助
    """
    text = text.strip()
    
    # 帮助
    if text in ("帮助", "help", "?", "？"):
        return ParsedCommand(cmd=CommandType.HELP, raw=text)

    # ── 查询 ──────────────────────────────────────────
    m = re.match(r'^(?:查询|查|query)\s+(.+)', text)
    if m:
        keyword = m.group(1).strip()
        cmd = ParsedCommand(cmd=CommandType.QUERY, raw=text)
        # 判断是会员号还是姓名
        if re.match(r'^[A-Za-z0-9]+$', keyword):
            cmd.member_id = keyword
        else:
            cmd.name = keyword
        return cmd

    # ── 录入 ──────────────────────────────────────────
    m = re.match(r'^(?:录入|新增|添加|register)\s+(.+)', text)
    if m:
        parts = m.group(1).strip().split()
        cmd = ParsedCommand(cmd=CommandType.REGISTER, raw=text)
        if len(parts) >= 1:
            cmd.name = parts[0]
        if len(parts) >= 2:
            cmd.card_name = parts[1]
        if len(parts) >= 3:
            cmd.payment_method = parts[2]
        if len(parts) >= 4:
            try:
                cmd.amount = float(parts[3])
            except ValueError:
                cmd.note = parts[3]
        if len(parts) >= 5:
            cmd.note = " ".join(parts[4:])
        return cmd

    # ── 扣课 ──────────────────────────────────────────
    m = re.match(r'^(?:扣课|签到|上课|扣|deduct)\s+(.+)', text)
    if m:
        parts = m.group(1).strip().split()
        cmd = ParsedCommand(cmd=CommandType.DEDUCT, raw=text)
        if len(parts) >= 1:
            cmd.name = parts[0]
        if len(parts) >= 2:
            try:
                cmd.deduct_count = int(parts[1])
            except ValueError:
                cmd.deduct_count = 1
                cmd.teacher = parts[1]
        if len(parts) >= 3:
            cmd.teacher = parts[2]
        if len(parts) >= 4:
            cmd.dance_type = parts[3]
        # 默认扣1次
        if cmd.deduct_count is None:
            cmd.deduct_count = 1
        return cmd

    # ── 续费 ──────────────────────────────────────────
    m = re.match(r'^(?:续费|续卡|renew)\s+(.+)', text)
    if m:
        parts = m.group(1).strip().split()
        cmd = ParsedCommand(cmd=CommandType.RENEW, raw=text)
        if len(parts) >= 1:
            cmd.name = parts[0]
        if len(parts) >= 2:
            cmd.card_name = parts[1]
        if len(parts) >= 3:
            cmd.payment_method = parts[2]
        if len(parts) >= 4:
            try:
                cmd.amount = float(parts[3])
            except ValueError:
                pass
        return cmd

    # ── 退卡 ──────────────────────────────────────────
    m = re.match(r'^(?:退卡|退款|refund)\s+(.+)', text)
    if m:
        parts = m.group(1).strip().split()
        cmd = ParsedCommand(cmd=CommandType.REFUND, raw=text)
        if len(parts) >= 1:
            cmd.name = parts[0]
        if len(parts) >= 2:
            try:
                cmd.amount = float(parts[1])
            except ValueError:
                cmd.note = parts[1]
        return cmd

    # ── 修改 ──────────────────────────────────────────
    m = re.match(r'^(?:修改|改|modify)\s+(.+)', text)
    if m:
        parts = m.group(1).strip().split(None, 2)
        cmd = ParsedCommand(cmd=CommandType.MODIFY, raw=text)
        if len(parts) >= 1:
            cmd.name = parts[0]
        if len(parts) >= 2:
            cmd.field_name = parts[1]
        if len(parts) >= 3:
            cmd.field_value = parts[2]
        return cmd

    # ── 撤销 ──────────────────────────────────────────
    m = re.match(r'^(?:撤销|撤|undo)\s+(.+)', text)
    if m:
        keyword = m.group(1).strip()
        cmd = ParsedCommand(cmd=CommandType.UNDO, raw=text)
        cmd.log_id = keyword
        # 也可能是按姓名撤销最近操作
        cmd.name = keyword
        return cmd

    # ── 统计 ──────────────────────────────────────────
    m = re.match(r'^(?:统计|stats?)\s*(.*)', text)
    if m:
        cmd = ParsedCommand(cmd=CommandType.STATS, raw=text)
        period = m.group(1).strip()
        if period:
            cmd.extra["period"] = period
        return cmd

    # ── 列表 ──────────────────────────────────────────
    m = re.match(r'^(?:列表|list)\s*(.*)', text)
    if m:
        cmd = ParsedCommand(cmd=CommandType.LIST, raw=text)
        sub = m.group(1).strip()
        if sub:
            cmd.extra["sub"] = sub
        return cmd

    # ── 提醒 ──────────────────────────────────────────
    m = re.match(r'^(?:提醒|remind)\s*(.*)', text)
    if m:
        cmd = ParsedCommand(cmd=CommandType.REMIND, raw=text)
        sub = m.group(1).strip()
        if sub:
            cmd.extra["sub"] = sub
        return cmd

    return ParsedCommand(cmd=CommandType.UNKNOWN, raw=text)


def format_help() -> str:
    """返回帮助信息"""
    return """🏄 7L街舞工作室机器人 - 命令帮助

📋 查询
  查询 张三        - 查询学员信息
  查询 会员号001   - 按会员号查询

📝 录入
  录入 张三 次卡·8次 微信 580
  录入 李四 体验卡 现金 49

✅ 扣课
  扣课 张三        - 扣1次课
  扣课 张三 2      - 扣2次课
  扣课 张三 1 王老师 hiphop

🔄 续费
  续费 张三 次卡·16次 微信 980

💰 退卡
  退卡 张三
  退卡 张三 200    - 指定退款金额

✏️ 修改
  修改 张三 电话 13800138000
  修改 张三 备注 VIP学员

↩️ 撤销
  撤销 张三        - 撤销该学员最近操作

📊 统计
  统计            - 本月统计
  统计 本月

📋 列表
  列表            - 有效学员列表
  列表 卡种       - 卡种定价

📸 截图
  直接发送接龙截图，机器人自动识别录入"""
