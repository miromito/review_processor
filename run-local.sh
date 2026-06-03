#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

COMPOSE_FILE="docker-compose-local.yml"
VENV_DIR=".venv"
PORT="${PORT:-8000}"
START_MONGO=1
MODE="native"

usage() {
  cat <<'EOF'
Локальный запуск review-analytics.

  ./run-local.sh              Python + uvicorn (--reload), MongoDB в Docker
  ./run-local.sh --docker     Весь стек в Docker (web + mongo, hot reload)
  ./run-local.sh --no-mongo   Только uvicorn (MongoDB уже запущена)
  ./run-local.sh --down       Остановить контейнеры compose

Перед первым запуском заполните .env (OPENAI_API_KEY и др.).
Сайт: http://127.0.0.1:8000/
EOF
}

load_env() {
  if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
  fi
}

ensure_env_file() {
  if [[ ! -f .env ]]; then
    cp .env.example .env
    echo "Создан .env из .env.example — задайте OPENAI_API_KEY и пароль Mongo (MONGO_INITDB_ROOT_PASSWORD)."
  fi
}

find_python() {
  if command -v python3.12 >/dev/null 2>&1; then
    echo "python3.12"
    return
  fi
  if python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' 2>/dev/null; then
    echo "python3"
    return
  fi
  echo "Ошибка: нужен Python 3.12 или новее." >&2
  exit 1
}

ensure_venv() {
  local python_bin
  python_bin="$(find_python)"

  if [[ ! -d "$VENV_DIR" ]]; then
    echo "Создаю виртуальное окружение в $VENV_DIR..."
    "$python_bin" -m venv "$VENV_DIR"
  fi

  echo "Устанавливаю зависимости..."
  "$VENV_DIR/bin/pip" install -q -r requirements.txt
}

require_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    echo "Ошибка: Docker не найден." >&2
    exit 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    echo "Ошибка: нужен Docker Compose v2 (команда docker compose)." >&2
    exit 1
  fi
}

start_mongo() {
  require_docker
  ensure_env_file
  load_env

  if [[ -z "${MONGO_INITDB_ROOT_USERNAME:-}" || -z "${MONGO_INITDB_ROOT_PASSWORD:-}" ]]; then
    echo "Ошибка: задайте MONGO_INITDB_ROOT_USERNAME и MONGO_INITDB_ROOT_PASSWORD в .env" >&2
    exit 1
  fi

  echo "Запуск MongoDB (Docker)..."
  docker compose -f "$COMPOSE_FILE" up -d --wait mongo
}

mongodb_uri_for_local_compose() {
  printf 'mongodb://%s:%s@localhost:27017/?authSource=admin' \
    "$MONGO_INITDB_ROOT_USERNAME" \
    "$MONGO_INITDB_ROOT_PASSWORD"
}

run_native() {
  ensure_env_file
  load_env

  if [[ "$START_MONGO" -eq 1 ]]; then
    start_mongo
    export MONGODB_URI
    MONGODB_URI="$(mongodb_uri_for_local_compose)"
  elif [[ -z "${MONGODB_URI:-}" ]]; then
    export MONGODB_URI="mongodb://localhost:27017"
  fi

  ensure_venv

  echo ""
  echo "Сервер: http://127.0.0.1:${PORT}/"
  echo "API docs: http://127.0.0.1:${PORT}/docs"
  echo ""

  exec "$VENV_DIR/bin/uvicorn" app.main:app --reload --host 127.0.0.1 --port "$PORT"
}

run_docker() {
  require_docker
  ensure_env_file
  exec docker compose -f "$COMPOSE_FILE" up --build
}

stop_services() {
  require_docker
  docker compose -f "$COMPOSE_FILE" down
  echo "Контейнеры остановлены."
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)
      MODE="docker"
      shift
      ;;
    --no-mongo)
      START_MONGO=0
      shift
      ;;
    --down)
      stop_services
      exit 0
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Неизвестный аргумент: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

case "$MODE" in
  native) run_native ;;
  docker) run_docker ;;
esac
