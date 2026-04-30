import secrets
from datetime import UTC, datetime, timedelta

from jose import JWTError, jwt

from app.config import Settings


def auth_enabled(settings: Settings) -> bool:
    return bool(settings.auth_username.strip())


def verify_credentials(username: str, password: str, settings: Settings) -> bool:
    if not auth_enabled(settings):
        return False
    u = username.encode("utf-8")
    p = password.encode("utf-8")
    eu = settings.auth_username.encode("utf-8")
    ep = settings.auth_password.encode("utf-8")
    if len(u) != len(eu) or len(p) != len(ep):
        return False
    return secrets.compare_digest(u, eu) and secrets.compare_digest(p, ep)


def create_access_token(settings: Settings) -> str:
    expire = datetime.now(UTC) + timedelta(minutes=settings.jwt_expire_minutes)
    payload = {"sub": settings.auth_username, "exp": int(expire.timestamp())}
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def verify_token(token: str, settings: Settings) -> bool:
    try:
        jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])
        return True
    except JWTError:
        return False
