"""7L街舞工作室飞书机器人 - 主应用"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from starlette.middleware.base import BaseHTTPMiddleware
import os
from app.routers.webhook import router as webhook_router
from app.routers.api import router as api_router
from app.services.scheduler import setup_scheduler, scheduler
from app.services.ai_service import init_ai_service
from app.config import settings

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期"""
    logger.info("🏄 7L Bot 启动中...")
    # 启动定时任务
    setup_scheduler()
    scheduler.start()
    logger.info("✅ 定时任务已启动")
    # 初始化AI服务
    init_ai_service()
    logger.info("✅ AI服务已初始化")
    yield
    scheduler.shutdown()
    logger.info("🏄 7L Bot 已停止")


app = FastAPI(
    title="7L街舞工作室机器人",
    description="飞书群机器人，管理学员、扣课、统计",
    version="1.0.0",
    lifespan=lifespan,
)

# API鉴权中间件 - 校验Bearer Token
SKIP_AUTH_PATHS = {"/", "/docs", "/openapi.json", "/redoc"}


class BearerAuthMiddleware(BaseHTTPMiddleware):
    """对所有 /api/v1/* 路由做 Bearer Token 校验（webhook 路径除外）"""

    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # 不需要鉴权的路径：根路径、文档、webhook（webhook 有自己的签名验证）
        if path in SKIP_AUTH_PATHS or path.startswith("/static") or path == "/app":
            return await call_next(request)

        # webhook 走签名验证，不走 Token
        if path.startswith("/api/v1/webhook"):
            return await call_next(request)

        # 其他 /api/v1/* 路径需要鉴权
        if path.startswith("/api/v1"):
            api_key = settings.API_KEY
            if not api_key:
                # 未配置 API_KEY 时跳过鉴权（开发模式）
                return await call_next(request)

            auth_header = request.headers.get("Authorization", "")
            if not auth_header.startswith("Bearer "):
                return Response(
                    content='{"detail":"未授权：缺少 Authorization header"}',
                    status_code=401,
                    media_type="application/json",
                )

            token = auth_header[7:]  # 去掉 "Bearer " 前缀
            if token != api_key:
                return Response(
                    content='{"detail":"未授权：Token 无效"}',
                    status_code=401,
                    media_type="application/json",
                )

        return await call_next(request)


app.add_middleware(BearerAuthMiddleware)

# CORS - 允许前端跨域访问（从环境变量读取允许的域名）
allowed_origins = [o.strip() for o in settings.ALLOWED_ORIGINS.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(webhook_router, prefix="/api/v1", tags=["webhook"])
app.include_router(api_router, prefix="/api/v1", tags=["api"])

# 静态文件服务（前端）- 7l-webapp 和 7l-bot 同级目录
WEBAPP_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '7l-webapp')
WEBAPP_DIR = os.path.normpath(WEBAPP_DIR)
if os.path.isdir(WEBAPP_DIR):
    app.mount("/static", StaticFiles(directory=WEBAPP_DIR), name="static")

    @app.get("/app")
    async def serve_app():
        """提供前端页面"""
        return FileResponse(os.path.join(WEBAPP_DIR, "index.html"))

    @app.get("/style.css")
    async def serve_css():
        return FileResponse(os.path.join(WEBAPP_DIR, "style.css"), media_type="text/css")

    @app.get("/app.js")
    async def serve_js():
        return FileResponse(os.path.join(WEBAPP_DIR, "app.js"), media_type="application/javascript")


@app.get("/")
async def root():
    return FileResponse(os.path.join(WEBAPP_DIR, "index.html"))


@app.get("/report")
async def serve_report():
    """纯静态数据报告（不依赖JS）"""
    return FileResponse(os.path.join(WEBAPP_DIR, "report.html"), media_type="text/html")
