"""飞书 Bitable API 客户端"""
import httpx
from app.config import settings
from app.services.feishu_client import feishu_client


class BitableClient:
    """飞书多维表格操作封装"""

    BASE_URL = "https://open.feishu.cn/open-apis/bitable/v1"

    def __init__(self):
        self._client = httpx.AsyncClient(timeout=30)

    async def _headers(self) -> dict:
        await feishu_client._ensure_token()
        return {"Authorization": f"Bearer {feishu_client._tenant_token}", "Content-Type": "application/json"}

    # ── 记录读取 ──────────────────────────────────────────

    async def list_records(self, app_token: str, table_id: str, page_size: int = 100,
                           filter_expr: str = None, sort: list = None) -> list:
        """列出记录，支持过滤和排序"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/records"
        params = {"page_size": page_size}
        if filter_expr:
            params["filter"] = filter_expr
        if sort:
            params["sort"] = str(sort)

        all_records = []
        page_token = None
        while True:
            if page_token:
                params["page_token"] = page_token
            resp = await self._client.get(url, headers=headers, params=params)
            data = resp.json()
            if data.get("code") != 0:
                raise RuntimeError(f"Bitable list_records 失败: {data}")
            items = data.get("data", {}).get("items") or []
            all_records.extend(items)
            page_token = data.get("data", {}).get("page_token")
            if not data.get("data", {}).get("has_more") or not page_token:
                break
        return all_records

    async def get_record(self, app_token: str, table_id: str, record_id: str) -> dict:
        """获取单条记录"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/records/{record_id}"
        resp = await self._client.get(url, headers=headers)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable get_record 失败: {data}")
        return data.get("data", {}).get("record", {})

    # ── 记录写入 ──────────────────────────────────────────

    async def create_record(self, app_token: str, table_id: str, fields: dict) -> dict:
        """创建记录"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/records"
        body = {"fields": fields}
        resp = await self._client.post(url, headers=headers, json=body)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable create_record 失败: {data}")
        return data.get("data", {}).get("record", {})

    async def update_record(self, app_token: str, table_id: str, record_id: str, fields: dict) -> dict:
        """更新记录"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/records/{record_id}"
        body = {"fields": fields}
        resp = await self._client.put(url, headers=headers, json=body)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable update_record 失败: {data}")
        return data.get("data", {}).get("record", {})

    async def delete_record(self, app_token: str, table_id: str, record_id: str) -> dict:
        """删除记录"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/records/{record_id}"
        resp = await self._client.delete(url, headers=headers)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable delete_record 失败: {data}")
        return data.get("data", {})

    async def batch_create_records(self, app_token: str, table_id: str, records: list[dict]) -> list:
        """批量创建记录"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/records/batch_create"
        body = {"records": [{"fields": r} for r in records]}
        resp = await self._client.post(url, headers=headers, json=body)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable batch_create 失败: {data}")
        return data.get("data", {}).get("records", [])

    # ── 便捷方法 ──────────────────────────────────────────

    async def find_member_by_name(self, name: str) -> list[dict]:
        """按姓名查找学员（主表）"""
        # 飞书Bitable filter语法
        filter_expr = f'CurrentValue.[姓名] = "{name}"'
        return await self.list_records(
            settings.BITABLE_MAIN_APP_TOKEN,
            settings.BITABLE_MAIN_TABLE_ID,
            filter_expr=filter_expr,
        )

    async def find_member_by_member_id(self, member_id: str) -> list[dict]:
        """按会员号查找学员"""
        filter_expr = f'CurrentValue.[会员号] = "{member_id}"'
        return await self.list_records(
            settings.BITABLE_MAIN_APP_TOKEN,
            settings.BITABLE_MAIN_TABLE_ID,
            filter_expr=filter_expr,
        )

    async def get_active_members(self) -> list[dict]:
        """获取所有有效卡学员"""
        filter_expr = 'CurrentValue.[卡状态] = "有效"'
        return await self.list_records(
            settings.BITABLE_MAIN_APP_TOKEN,
            settings.BITABLE_MAIN_TABLE_ID,
            filter_expr=filter_expr,
        )

    async def get_pricing_list(self) -> list[dict]:
        """获取卡种定价列表"""
        return await self.list_records(
            settings.BITABLE_PRICING_APP_TOKEN,
            settings.BITABLE_PRICING_TABLE_ID,
        ) or []

    async def add_class_record(self, fields: dict) -> dict:
        """添加上课登记记录"""
        return await self.create_record(
            settings.BITABLE_CLASS_APP_TOKEN,
            settings.BITABLE_CLASS_TABLE_ID,
            fields,
        )

    async def add_log(self, operator: str, op_type: str, student_name: str, detail: str):
        """添加操作日志"""
        import time
        fields = {
            "7L-操作日志": f"{op_type}-{student_name}",
            "操作时间": int(time.time() * 1000),
            "操作人": operator,
            "操作类型": op_type,
            "学员姓名": student_name,
            "变更详情": detail,
            "是否已撤销": False,
        }
        return await self.create_record(
            settings.BITABLE_LOG_APP_TOKEN,
            settings.BITABLE_LOG_TABLE_ID,
            fields,
        )

    async def get_recent_logs(self, student_name: str = None, limit: int = 10) -> list[dict]:
        """获取最近操作日志"""
        filter_expr = None
        if student_name:
            filter_expr = f'CurrentValue.[学员姓名] = "{student_name}"'
        records = await self.list_records(
            settings.BITABLE_LOG_APP_TOKEN,
            settings.BITABLE_LOG_TABLE_ID,
            filter_expr=filter_expr,
        ) or []
        # 按操作时间倒序
        records.sort(key=lambda r: r.get("fields", {}).get("操作时间", 0), reverse=True)
        return records[:limit]

    # ── 老师管理 ──────────────────────────────────────────

    async def list_tables(self, app_token: str) -> list[dict]:
        """列出Bitable应用下的所有表"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables"
        resp = await self._client.get(url, headers=headers)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable list_tables 失败: {data}")
        return data.get("data", {}).get("items", [])

    async def create_table(self, app_token: str, table_name: str, fields: list[dict]) -> dict:
        """在Bitable应用下创建新表"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables"
        body = {
            "table": {
                "name": table_name,
                "default_view_name": "默认视图",
                "fields": fields,
            }
        }
        resp = await self._client.post(url, headers=headers, json=body)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable create_table 失败: {data}")
        return data.get("data", {})

    async def create_field(self, app_token: str, table_id: str, field_name: str, field_type: int, property: dict = None) -> dict:
        """在表中创建字段"""
        headers = await self._headers()
        url = f"{self.BASE_URL}/apps/{app_token}/tables/{table_id}/fields"
        body = {"field_name": field_name, "type": field_type}
        if property:
            body["property"] = property
        resp = await self._client.post(url, headers=headers, json=body)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"Bitable create_field 失败: {data}")
        return data.get("data", {}).get("field", {})

    async def get_teachers(self) -> list[dict]:
        """获取老师列表"""
        if not settings.BITABLE_TEACHER_APP_TOKEN or not settings.BITABLE_TEACHER_TABLE_ID:
            return []
        return await self.list_records(
            settings.BITABLE_TEACHER_APP_TOKEN,
            settings.BITABLE_TEACHER_TABLE_ID,
        ) or []

    async def create_teacher(self, fields: dict) -> dict:
        """新增老师"""
        return await self.create_record(
            settings.BITABLE_TEACHER_APP_TOKEN,
            settings.BITABLE_TEACHER_TABLE_ID,
            fields,
        )

    async def update_teacher(self, record_id: str, fields: dict) -> dict:
        """更新老师信息"""
        return await self.update_record(
            settings.BITABLE_TEACHER_APP_TOKEN,
            settings.BITABLE_TEACHER_TABLE_ID,
            record_id,
            fields,
        )


# 全局单例
bitable = BitableClient()
