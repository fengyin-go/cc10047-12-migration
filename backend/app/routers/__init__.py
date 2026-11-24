"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import migration, resource, chains

# 迁移路由含 /api/migration 固定前缀，先注册，避免被资源路由的动态路径抢先匹配。
ROUTERS = [migration, resource, chains]
