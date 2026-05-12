"""7L街舞工作室飞书机器人 - 配置管理"""
import os
from dotenv import load_dotenv

load_dotenv()


class Settings:
    # 飞书
    FEISHU_APP_ID: str = os.getenv("FEISHU_APP_ID", "")
    FEISHU_APP_SECRET: str = os.getenv("FEISHU_APP_SECRET", "")
    WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")

    # 百度OCR
    BAIDU_OCR_API_KEY: str = os.getenv("BAIDU_OCR_API_KEY", "")
    BAIDU_OCR_SECRET_KEY: str = os.getenv("BAIDU_OCR_SECRET_KEY", "")

    # Bitable - 主表（学员卡信息）
    BITABLE_MAIN_APP_TOKEN: str = os.getenv("BITABLE_MAIN_APP_TOKEN", "M2BpbbL9SaR5H5svaqUc5Qnln3c")
    BITABLE_MAIN_TABLE_ID: str = os.getenv("BITABLE_MAIN_TABLE_ID", "tblIqPj4SFs33nXu")

    # Bitable - 上课登记
    BITABLE_CLASS_APP_TOKEN: str = os.getenv("BITABLE_CLASS_APP_TOKEN", "XsY3bx0eaaQJFispgakcHq43nke")
    BITABLE_CLASS_TABLE_ID: str = os.getenv("BITABLE_CLASS_TABLE_ID", "tbl1dtChwHjbSP0A")

    # Bitable - 卡种定价
    BITABLE_PRICING_APP_TOKEN: str = os.getenv("BITABLE_PRICING_APP_TOKEN", "As3obylE9awP2psAvKgcy3GqnZc")
    BITABLE_PRICING_TABLE_ID: str = os.getenv("BITABLE_PRICING_TABLE_ID", "tblydEatu4oHRtJj")

    # Bitable - 操作日志
    BITABLE_LOG_APP_TOKEN: str = os.getenv("BITABLE_LOG_APP_TOKEN", "EvTVblZp1atiazsRTcJcWE2vn4g")
    BITABLE_LOG_TABLE_ID: str = os.getenv("BITABLE_LOG_TABLE_ID", "tblK7cj3NIsKjKPk")

    # Bitable - 老师管理
    BITABLE_TEACHER_APP_TOKEN: str = os.getenv("BITABLE_TEACHER_APP_TOKEN", "OE3DbNONbaDaESs8JiPcDAyinWV")
    BITABLE_TEACHER_TABLE_ID: str = os.getenv("BITABLE_TEACHER_TABLE_ID", "tblWdy1W4UwNYdp5")

    # API鉴权
    API_KEY: str = os.getenv("API_KEY", "")

    # AI助手 - 通过OpenClaw Gateway（复用已付费模型，零额外成本）
    OPENCLAW_GATEWAY_URL: str = os.getenv("OPENCLAW_GATEWAY_URL", "ws://127.0.0.1:18789")
    OPENCLAW_GATEWAY_TOKEN: str = os.getenv("OPENCLAW_GATEWAY_TOKEN", "")

    # CORS
    ALLOWED_ORIGINS: str = os.getenv("ALLOWED_ORIGINS", "*")

    # 服务
    PORT: int = int(os.getenv("PORT", "8000"))


settings = Settings()
