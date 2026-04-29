import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from motor.motor_asyncio import AsyncIOMotorClient

from app.auth_jwt import auth_enabled, verify_token
from app.config import get_settings
from app.db import ensure_indexes
from app.routers import api_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory="app/templates")


def _template_show_auth_nav() -> bool:
    return auth_enabled(get_settings())


templates.env.globals["show_auth_nav"] = _template_show_auth_nav


def _public_route(path: str, method: str) -> bool:
    if path.startswith("/static/"):
        return True
    if path == "/login":
        return True
    m = method.upper()
    if path == "/api/auth/login" and m == "POST":
        return True
    if path == "/api/auth/logout" and m == "POST":
        return True
    return False


def _request_token(request: Request) -> str | None:
    raw = request.cookies.get("access_token")
    if raw:
        return raw
    auth = request.headers.get("authorization") or ""
    prefix = "bearer "
    if len(auth) > len(prefix) and auth[: len(prefix)].lower() == prefix:
        return auth[len(prefix) :].strip()
    return None


_STATIC_ROOT = Path(__file__).resolve().parent.parent / "static"


def _project_detail_js_mtime() -> str:
    """Версия для query string — сбрасывает кэш браузера после правок JS."""
    try:
        return str(int((_STATIC_ROOT / "project_detail.js").stat().st_mtime))
    except OSError:
        return "0"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    client = AsyncIOMotorClient(settings.mongodb_uri)
    app.state.mongo_client = client
    app.state.db = client[settings.mongodb_db]
    try:
        await ensure_indexes(app.state.db)
        logger.info("MongoDB: индексы проверены")
    except Exception:
        logger.exception("MongoDB недоступен при старте — повторите при запущенной БД")
    yield
    client.close()


app = FastAPI(title="Анализ отзывов", lifespan=lifespan)


@app.middleware("http")
async def jwt_cookie_auth(request: Request, call_next):
    settings = get_settings()
    if not auth_enabled(settings):
        return await call_next(request)
    if _public_route(request.url.path, request.method):
        return await call_next(request)
    token = _request_token(request)
    if token and verify_token(token, settings):
        return await call_next(request)
    if request.url.path.startswith("/api"):
        return JSONResponse({"detail": "Требуется вход"}, status_code=401)
    return RedirectResponse(url="/login", status_code=302)


app.mount("/static", StaticFiles(directory="static"), name="static")
app.include_router(api_router, prefix="/api")


@app.get("/", response_class=HTMLResponse)
async def projects_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "projects.html", {"title": "Проекты"})


@app.get("/projects/new", response_class=HTMLResponse)
async def project_new_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "project_new.html", {"title": "Новый проект"})


@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    settings = get_settings()
    if not auth_enabled(settings):
        return RedirectResponse(url="/", status_code=302)
    return templates.TemplateResponse(request, "login.html", {"title": "Вход"})


@app.get("/projects/{project_id}", response_class=HTMLResponse)
async def project_detail_page(request: Request, project_id: str) -> HTMLResponse:
    settings = get_settings()
    return templates.TemplateResponse(
        request,
        "project_detail.html",
        {
            "title": "Проект",
            "project_id": project_id,
            "token_limit_t": settings.token_limit_t,
            "openai_model": settings.openai_model,
            "project_detail_js_v": _project_detail_js_mtime(),
        },
    )
