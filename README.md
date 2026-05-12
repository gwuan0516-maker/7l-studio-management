# 7L 街舞工作室学员管理系统

## 项目结构
- `frontend/` — 前端（HTML + CSS + JS，移动端优先）
- `backend/` — 后端（Python FastAPI + 飞书Bitable）
- `nginx/` — Nginx配置
- `.github/workflows/` — CI/CD自动部署

## 部署
推代码到main分支 → GitHub Actions自动部署到服务器

## 本地开发
```bash
# 前端
cd frontend && python3 -m http.server 3000

# 后端
cd backend && pip install -r requirements.txt && python run.py
```
