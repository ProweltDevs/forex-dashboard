"""Email channel (SPEC §14, optional): SMTP with STARTTLS, run in a worker thread (smtplib blocks).
Default minimum severity is WARNING: email is for things worth reading later."""

from __future__ import annotations

import asyncio
import smtplib
import ssl
from email.message import EmailMessage

from .notifier import ChannelError, Severity


class EmailChannel:
    name = "email"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        sender: str,
        recipients: tuple[str, ...],
        user: str | None,
        password: str | None,
        starttls: bool = True,
        min_severity: Severity = Severity.WARNING,
        timeout_s: float = 20.0,
    ) -> None:
        self._host, self._port, self._sender, self._to = host, port, sender, recipients
        self._user, self._password, self._starttls = user, password, starttls
        self.min_severity = min_severity
        self._timeout = timeout_s

    def _send(self, subject: str, text: str) -> None:
        msg = EmailMessage()
        msg["From"] = self._sender
        msg["To"] = ", ".join(self._to)
        msg["Subject"] = subject
        msg.set_content(text)
        with smtplib.SMTP(self._host, self._port, timeout=self._timeout) as smtp:
            if self._starttls:
                smtp.starttls(context=ssl.create_default_context())
            if self._user and self._password:
                smtp.login(self._user, self._password)
            smtp.send_message(msg)

    async def send(self, subject: str, text: str) -> None:
        try:
            await asyncio.to_thread(self._send, subject, text)
        except (smtplib.SMTPException, OSError) as exc:  # no exception text: it may echo credentials
            raise ChannelError(f"smtp {self._host}:{self._port}: {type(exc).__name__}") from None

    async def close(self) -> None:
        return None
