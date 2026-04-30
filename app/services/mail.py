import asyncio
import logging
import smtplib
import ssl
from email.message import EmailMessage

from app.config import Settings

logger = logging.getLogger(__name__)


def send_notification_email(
    settings: Settings,
    *,
    to_addr: str,
    subject: str,
    body_text: str,
) -> bool:
    to_addr = (to_addr or "").strip()
    if not to_addr:
        return False
    host = (settings.smtp_host or "").strip()
    if not host:
        logger.info("SMTP не настроен (SMTP_HOST пуст) — письмо на %s не отправлено", to_addr)
        return False
    mail_from = (settings.mail_from or settings.smtp_user or "").strip()
    if not mail_from:
        logger.warning("MAIL_FROM / SMTP_USER пусты — письмо не отправлено")
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_addr
    msg.set_content(body_text)
    try:
        if settings.smtp_use_ssl:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(host, settings.smtp_port, context=context) as smtp:
                if settings.smtp_user and settings.smtp_password:
                    smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, settings.smtp_port) as smtp:
                if settings.smtp_use_starttls:
                    context = ssl.create_default_context()
                    smtp.starttls(context=context)
                if settings.smtp_user and settings.smtp_password:
                    smtp.login(settings.smtp_user, settings.smtp_password)
                smtp.send_message(msg)
    except OSError as e:
        logger.exception("Ошибка отправки письма: %s", e)
        return False
    return True


async def send_notification_email_async(
    settings: Settings,
    *,
    to_addr: str,
    subject: str,
    body_text: str,
) -> bool:
    return await asyncio.to_thread(
        send_notification_email,
        settings,
        to_addr=to_addr,
        subject=subject,
        body_text=body_text,
    )
