from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from Api.routes import router as users_router

app = FastAPI(title="Agent_4K API")
app.include_router(users_router)

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"

app.mount("/web", StaticFiles(directory=WEB_DIR), name="web")


@app.get("/")
def read_root() -> FileResponse:
    return FileResponse(WEB_DIR / "index.html")
