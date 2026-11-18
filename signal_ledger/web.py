"""仅暴露离线回放和只读展示接口，不提供凭据、实盘启动或交易参数接口。"""

from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse

from .demo_runtime import DemoSession
from .network_guard import install_loopback_only


def create_app():
    @asynccontextmanager
    async def lifespan(app):
        install_loopback_only()
        session = DemoSession()
        app.state.session = session
        try:
            await session.replay()
            yield
        finally:
            session.close()

    app = FastAPI(title="基于 Python 的多交易所信号执行与账本管理系统", lifespan=lifespan)

    @app.middleware("http")
    async def local_requests_only(request: Request, call_next):
        # 拒绝外站发起的写请求，避免浏览网页时被跨站触发演示操作。
        if request.method not in ("GET", "HEAD", "OPTIONS"):
            origin = request.headers.get("origin")
            expected = str(request.base_url).rstrip("/")
            if origin and origin.rstrip("/") != expected:
                from fastapi.responses import JSONResponse
                return JSONResponse({"detail": "仅接受同源演示操作"}, status_code=403)
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.get("/", response_class=HTMLResponse)
    async def index():
        return (Path(__file__).parent / "static" / "index.html").read_text(encoding="utf-8")

    @app.get("/api/status")
    async def status(exchange: str = ""):
        if exchange not in ("", "gate", "bitget"):
            raise HTTPException(status_code=422, detail="不支持的演示交易所")
        return app.state.session.snapshot(exchange)

    @app.post("/api/demo/replay")
    async def replay():
        return await app.state.session.replay()

    return app
