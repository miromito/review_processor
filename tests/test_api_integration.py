"""Интеграционные тесты HTTP (ASGI) и MongoDB.

Пропускаются целиком, если на ``127.0.0.1:27017`` нет ``mongod``:
юнит-тесты при этом по-прежнему выполняются на любой машине.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest
import pytest_asyncio


def _mongo_reachable() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1.5):
            return True
    except OSError:
        return False


if not _mongo_reachable():
    pytest.skip(
        "Нужен MongoDB на 127.0.0.1:27017 для интеграционных тестов",
        allow_module_level=True,
    )

os.environ["MONGODB_DB"] = f"review_analytics_pytest_{uuid.uuid4().hex[:12]}"
os.environ["AUTH_USERNAME"] = ""

from app.config import get_settings  # noqa: E402

get_settings.cache_clear()

from httpx import ASGITransport, AsyncClient  # noqa: E402

from app.main import app  # noqa: E402


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(
        transport=ASGITransport(app=app, lifespan="on"),
        base_url="http://test",
    ) as ac:
        yield ac


@pytest.mark.asyncio
async def test_index_page_returns_html(client):
    response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_project_crud_http_roundtrip(client):
    response = await client.post("/api/projects", json={"name": "pytest-проект"})
    assert response.status_code == 200
    body = response.json()
    project_id = body["project_id"]
    assert body["phase"] == "awaiting_file"

    response = await client.get("/api/projects")
    assert response.status_code == 200
    known = {item["id"] for item in response.json()}
    assert project_id in known

    response = await client.get(f"/api/projects/{project_id}")
    assert response.status_code == 200
    assert response.json()["name"] == "pytest-проект"

    response = await client.patch(
        f"/api/projects/{project_id}/name",
        json={"name": "переименован"},
    )
    assert response.status_code == 200
    assert response.json()["name"] == "переименован"

    response = await client.delete(f"/api/projects/{project_id}")
    assert response.status_code == 204

    response = await client.get(f"/api/projects/{project_id}")
    assert response.status_code == 404
