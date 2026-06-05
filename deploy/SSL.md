# HTTPS на Droplet (nginx + Let's Encrypt)

Домен: **анализ-отзывов.рф** (punycode: `xn----7sbaja1abf0asqc3c3h.xn--p1ai`)

Приложение в Docker слушает только `127.0.0.1:8000`. Снаружи — nginx на портах 80/443.

## Перед началом

1. DNS: A-запись `@` (и при необходимости `www`) → IP Droplet.
2. В DigitalOcean → Networking → Firewall (если есть): открыты **80** и **443** на Droplet.

## На сервере (один раз)

```bash
cd /var/www/app/review_processor
git pull origin main

# Обновить .env для HTTPS (см. ниже), затем:
export APP_VERSION="$(git rev-parse --short HEAD)"
docker compose up -d --build

sudo apt update
sudo apt install -y nginx certbot python3-certbot-nginx

sudo cp deploy/nginx/review-analytics.conf /etc/nginx/sites-available/review-analytics
sudo ln -sf /etc/nginx/sites-available/review-analytics /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl reload nginx

sudo certbot --nginx \
  -d xn----7sbaja1abf0asqc3c3h.xn--p1ai \
  --email ВАШ_EMAIL \
  --agree-tos \
  --redirect

sudo certbot renew --dry-run
```

### `.env` на сервере

```env
APP_BASE_URL=https://анализ-отзывов.рф
COOKIE_SECURE=true
```

Если включён JWT-вход (`AUTH_USERNAME` + `JWT_SECRET`), без `COOKIE_SECURE=true` cookie не установится по HTTPS.

После правки `.env`:

```bash
docker compose up -d
```

## Проверка

- https://анализ-отзывов.рф открывается
- http://… редиректит на https
- `sudo certbot renew --dry-run` без ошибок
