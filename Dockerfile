FROM python:3.12-slim

WORKDIR /app

ARG APP_VERSION=local
ENV APP_VERSION=${APP_VERSION}

RUN pip install --no-cache-dir --upgrade pip

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY static ./static

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
