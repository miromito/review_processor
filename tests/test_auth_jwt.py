"""Юнит-тесты для JWT-аутентификации без поднятого HTTP-сервера."""

from app.auth_jwt import auth_enabled, create_access_token, verify_credentials, verify_token
from app.config import Settings


def _settings_auth_off() -> Settings:
    return Settings(
        mongodb_uri="mongodb://127.0.0.1:27017",
        mongodb_db="test",
        auth_username="",
        auth_password="",
        jwt_secret="",
    )


def _settings_auth_on() -> Settings:
    return Settings(
        mongodb_uri="mongodb://127.0.0.1:27017",
        mongodb_db="test",
        auth_username="demo_user",
        auth_password="demo_pass",
        jwt_secret="unit_test_secret_key_must_be_long_enough_hs256",
    )


def test_auth_disabled_when_username_empty():
    s = _settings_auth_off()
    assert auth_enabled(s) is False
    assert verify_credentials("any", "any", s) is False


def test_auth_enabled_and_verify_credentials():
    s = _settings_auth_on()
    assert auth_enabled(s) is True
    assert verify_credentials("demo_user", "demo_pass", s) is True
    assert verify_credentials("demo_user", "wrong", s) is False
    assert verify_credentials("other", "demo_pass", s) is False


def test_jwt_round_trip():
    s = _settings_auth_on()
    token = create_access_token(s)
    assert verify_token(token, s) is True


def test_invalid_token_rejected():
    s = _settings_auth_on()
    assert verify_token("not-a-jwt", s) is False
