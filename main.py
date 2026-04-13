from pathlib import Path

from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from Api.routes import router as users_router
from Api.web_session_service import web_session_service

app = FastAPI(title="Agent_4K API")
app.include_router(users_router)

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"

web_session_service.ensure_schema()

app.mount("/web", StaticFiles(directory=WEB_DIR), name="web")


@app.middleware("http")
async def disable_browser_cache(request: Request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/web/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.get("/")
def read_root() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")
