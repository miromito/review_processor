from fastapi import APIRouter, Depends, HTTPException, Response
from pydantic import BaseModel

from app.auth_jwt import auth_enabled, create_access_token, verify_credentials
from app.config import Settings, get_settings

router = APIRouter()


class LoginBody(BaseModel):
    username: str = ""
    password: str = ""


def _settings_dep() -> Settings:
    return get_settings()


@router.post("/login")
async def login(response: Response, body: LoginBody, settings: Settings = Depends(_settings_dep)):
    if not auth_enabled(settings):
        raise HTTPException(503, "Вход в приложение сейчас отключён.")
    if not verify_credentials(body.username, body.password, settings):
        raise HTTPException(401, "Неверный логин или пароль")
    token = create_access_token(settings)
    max_age = settings.jwt_expire_minutes * 60
    response.set_cookie(
        key="access_token",
        value=token,
        httponly=True,
        max_age=max_age,
        samesite="lax",
        path="/",
        secure=settings.cookie_secure,
    )
    return {"ok": True}


@router.post("/logout")
async def logout(response: Response, settings: Settings = Depends(_settings_dep)):
    response.delete_cookie(key="access_token", path="/", samesite="lax", secure=settings.cookie_secure)
    return {"ok": True}
