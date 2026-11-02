"""Privacy-safe, stage-level SMTP transport diagnostics."""

import ipaddress
import logging
import socket
import smtplib
import ssl
import time
from email.mime.text import MIMEText
from urllib.parse import unquote, urlparse

import socks


logger = logging.getLogger(__name__)

# Clash/Mihomo-style TUN proxies answer DNS with addresses from this benchmark
# range, so a "direct" SMTP connection is silently routed through the proxy.
_PROXY_FAKE_IP_NETWORK = ipaddress.ip_network("198.18.0.0/15")


def is_proxy_fake_ip(address: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address((address or "").split("%", 1)[0])
    except ValueError:
        return False
    return ip_obj.version == 4 and ip_obj in _PROXY_FAKE_IP_NETWORK


def _addresses_use_proxy_fake_ip(addresses) -> bool:
    for item in addresses:
        sockaddr = item[4] if len(item) > 4 else ()
        if sockaddr and is_proxy_fake_ip(str(sockaddr[0])):
            return True
    return False


def proxy_summary(proxy_url: str | None) -> str:
    """Return a credential-free proxy description suitable for logs."""
    parsed = urlparse((proxy_url or "").strip())
    if not parsed.scheme or not parsed.hostname:
        return "none"
    host_kind = "loopback" if parsed.hostname.lower() in {"localhost", "127.0.0.1", "::1"} else "remote"
    return f"{parsed.scheme.lower()}:{host_kind}:{parsed.port or 'default'}"


def _proxy_socket(proxy_url: str, host: str, port: int, timeout: int, source_address=None):
    parsed = urlparse((proxy_url or "").strip())
    proxy_types = {
        "http": socks.HTTP,
        "socks5": socks.SOCKS5,
        "socks5h": socks.SOCKS5,
    }
    proxy_type = proxy_types.get(parsed.scheme.lower())
    if proxy_type is None:
        raise ValueError(f"Unsupported SMTP proxy scheme: {parsed.scheme or 'missing'}")
    if not parsed.hostname:
        raise ValueError("SMTP proxy URL has no host")

    sock = socks.socksocket()
    sock.settimeout(timeout)
    if source_address:
        sock.bind(source_address)
    sock.set_proxy(
        proxy_type,
        parsed.hostname,
        parsed.port,
        rdns=parsed.scheme.lower() != "socks5",
        username=unquote(parsed.username) if parsed.username else None,
        password=unquote(parsed.password) if parsed.password else None,
    )
    sock.connect((host, port))
    return sock


def _create_connection(host, port, timeout, source_address, proxy_url):
    if proxy_url:
        return _proxy_socket(proxy_url, host, port, timeout, source_address)
    return socket.create_connection((host, port), timeout, source_address)


def elapsed_ms(started: float) -> int:
    return max(0, round((time.perf_counter() - started) * 1000))


def email_domain(value: str) -> str:
    candidate = (value or "").strip().lower()
    return candidate.rsplit("@", 1)[-1] if "@" in candidate else "invalid"


def _exception_fields(exc: Exception) -> tuple[str, str, str, str]:
    smtp_code = getattr(exc, "smtp_code", "")
    smtp_error = getattr(exc, "smtp_error", b"")
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        smtp_error = b"recipient details redacted"
    if isinstance(smtp_error, bytes):
        smtp_error = smtp_error.decode("utf-8", errors="replace")
    safe_error = str(smtp_error or "").replace("\r", " ").replace("\n", " ")[:200]
    return (
        str(smtp_code or "none"),
        safe_error or "none",
        str(getattr(exc, "errno", "") or "none"),
        str(getattr(exc, "winerror", "") or "none"),
    )


def log_stage_success(
    *,
    trace_id: str,
    channel: str,
    attempt: int,
    stage: str,
    started: float,
    details: str = "",
) -> None:
    logger.info(
        "SMTP_DIAG stage_result trace_id=%s channel=%s attempt=%d stage=%s result=success elapsed_ms=%d%s",
        trace_id,
        channel,
        attempt,
        stage,
        elapsed_ms(started),
        f" {details}" if details else "",
    )


def log_stage_failure(
    exc: Exception,
    *,
    trace_id: str,
    channel: str,
    attempt: int,
    stage: str,
    started: float,
    port: int,
) -> None:
    smtp_code, smtp_error, errno_value, winerror_value = _exception_fields(exc)
    logger.warning(
        "SMTP_DIAG stage_result trace_id=%s channel=%s attempt=%d stage=%s result=failed "
        "elapsed_ms=%d port=%d exception=%s smtp_code=%s smtp_error=%s errno=%s winerror=%s",
        trace_id,
        channel,
        attempt,
        stage,
        elapsed_ms(started),
        port,
        type(exc).__name__,
        smtp_code,
        smtp_error,
        errno_value,
        winerror_value,
    )


class DiagnosticSMTP(smtplib.SMTP):
    def __init__(self, *args, trace_id: str, channel: str, attempt: int, proxy_url: str = "", **kwargs):
        self._diag_trace_id = trace_id
        self._diag_channel = channel
        self._diag_attempt = attempt
        self._diag_proxy_url = proxy_url
        super().__init__(*args, **kwargs)

    def _get_socket(self, host, port, timeout):
        started = time.perf_counter()
        try:
            sock = _create_connection(
                host, port, timeout, getattr(self, "source_address", None), self._diag_proxy_url
            )
        except Exception as exc:
            log_stage_failure(
                exc,
                trace_id=self._diag_trace_id,
                channel=self._diag_channel,
                attempt=self._diag_attempt,
                stage="tcp_connect",
                started=started,
                port=port,
            )
            raise
        peer_family = "ipv6" if sock.family == socket.AF_INET6 else "ipv4"
        log_stage_success(
            trace_id=self._diag_trace_id,
            channel=self._diag_channel,
            attempt=self._diag_attempt,
            stage="tcp_connect",
            started=started,
            details=f"port={port} peer_family={peer_family} route={'proxy' if self._diag_proxy_url else 'direct'}",
        )
        return sock


class DiagnosticSMTPSSL(smtplib.SMTP_SSL):
    def __init__(self, *args, trace_id: str, channel: str, attempt: int, proxy_url: str = "", **kwargs):
        self._diag_trace_id = trace_id
        self._diag_channel = channel
        self._diag_attempt = attempt
        self._diag_proxy_url = proxy_url
        super().__init__(*args, **kwargs)

    def _get_socket(self, host, port, timeout):
        tcp_started = time.perf_counter()
        try:
            raw_socket = _create_connection(
                host, port, timeout, getattr(self, "source_address", None), self._diag_proxy_url
            )
        except Exception as exc:
            log_stage_failure(
                exc,
                trace_id=self._diag_trace_id,
                channel=self._diag_channel,
                attempt=self._diag_attempt,
                stage="tcp_connect",
                started=tcp_started,
                port=port,
            )
            raise
        peer_family = "ipv6" if raw_socket.family == socket.AF_INET6 else "ipv4"
        log_stage_success(
            trace_id=self._diag_trace_id,
            channel=self._diag_channel,
            attempt=self._diag_attempt,
            stage="tcp_connect",
            started=tcp_started,
            details=f"port={port} peer_family={peer_family} route={'proxy' if self._diag_proxy_url else 'direct'}",
        )

        tls_started = time.perf_counter()
        try:
            wrapped = self.context.wrap_socket(raw_socket, server_hostname=self._host)
        except Exception as exc:
            raw_socket.close()
            log_stage_failure(
                exc,
                trace_id=self._diag_trace_id,
                channel=self._diag_channel,
                attempt=self._diag_attempt,
                stage="tls_handshake",
                started=tls_started,
                port=port,
            )
            raise
        cipher = wrapped.cipher()
        log_stage_success(
            trace_id=self._diag_trace_id,
            channel=self._diag_channel,
            attempt=self._diag_attempt,
            stage="tls_handshake",
            started=tls_started,
            details=(
                f"port={port} tls_version={wrapped.version() or 'unknown'} "
                f"cipher={(cipher[0] if cipher else 'unknown')}"
            ),
        )
        return wrapped


def send_smtp_once(
    config,
    smtp_password: str,
    msg: MIMEText,
    recipients: list[str],
    *,
    port: int,
    trace_id: str,
    channel: str,
    attempt: int,
    timeout: int,
    proxy_url: str = "",
    data_timeout: int | None = None,
) -> None:
    """Send once and log the precise failing transport stage.

    ``timeout`` bounds connection setup (TCP, TLS, greeting, EHLO, login);
    ``data_timeout`` replaces it after login so large reports are not cut off.
    A failed QUIT after successful DATA is logged but does not turn an accepted
    message into a failure, preventing duplicate retries after server acceptance.
    Failures after a proxy fake-ip DNS answer carry ``proxy_intercepted = True``.
    """
    server = None
    accepted = False
    route_intercepted = False
    stage = "dns_lookup"
    stage_started = time.perf_counter()
    try:
        if proxy_url:
            log_stage_success(
                trace_id=trace_id,
                channel=channel,
                attempt=attempt,
                stage=stage,
                started=stage_started,
                details=f"port={port} mode=proxy_remote proxy={proxy_summary(proxy_url)}",
            )
        else:
            addresses = socket.getaddrinfo(config.smtp_server, port, type=socket.SOCK_STREAM)
            families = sorted({"ipv6" if item[0] == socket.AF_INET6 else "ipv4" for item in addresses})
            route_intercepted = _addresses_use_proxy_fake_ip(addresses)
            log_stage_success(
                trace_id=trace_id,
                channel=channel,
                attempt=attempt,
                stage=stage,
                started=stage_started,
                details=(
                    f"port={port} mode=local address_count={len(addresses)} "
                    f"families={','.join(families) or 'none'} "
                    f"proxy_fake_ip={str(route_intercepted).lower()}"
                ),
            )
            if route_intercepted:
                logger.warning(
                    "SMTP_DIAG route_intercepted trace_id=%s channel=%s attempt=%d server=%s port=%d "
                    "reason=dns_returned_proxy_fake_ip direct_route_goes_through_local_tun_proxy=true",
                    trace_id,
                    channel,
                    attempt,
                    config.smtp_server,
                    port,
                )

        context = ssl.create_default_context()
        stage = "smtp_greeting"
        stage_started = time.perf_counter()
        if port == 465:
            server = DiagnosticSMTPSSL(
                config.smtp_server,
                port,
                timeout=timeout,
                context=context,
                trace_id=trace_id,
                channel=channel,
                attempt=attempt,
                proxy_url=proxy_url,
            )
        else:
            server = DiagnosticSMTP(
                config.smtp_server,
                port,
                timeout=timeout,
                trace_id=trace_id,
                channel=channel,
                attempt=attempt,
                proxy_url=proxy_url,
            )
        server.set_debuglevel(0)
        log_stage_success(
            trace_id=trace_id,
            channel=channel,
            attempt=attempt,
            stage=stage,
            started=stage_started,
            details=f"port={port}",
        )

        stage = "ehlo"
        stage_started = time.perf_counter()
        code, _response = server.ehlo()
        log_stage_success(
            trace_id=trace_id,
            channel=channel,
            attempt=attempt,
            stage=stage,
            started=stage_started,
            details=f"port={port} smtp_code={code} extension_count={len(server.esmtp_features)}",
        )

        if port != 465:
            starttls_advertised = server.has_extn("STARTTLS")
            if port == 587 and not starttls_advertised:
                raise smtplib.SMTPNotSupportedError("STARTTLS is not advertised by the SMTP server")
            if starttls_advertised:
                stage = "tls_handshake"
                stage_started = time.perf_counter()
                code, _response = server.starttls(context=context)
                cipher = server.sock.cipher() if server.sock else None
                tls_version = server.sock.version() if server.sock else None
                log_stage_success(
                    trace_id=trace_id,
                    channel=channel,
                    attempt=attempt,
                    stage=stage,
                    started=stage_started,
                    details=(
                        f"port={port} smtp_code={code} tls_version={tls_version or 'unknown'} "
                        f"cipher={(cipher[0] if cipher else 'unknown')}"
                    ),
                )
                stage = "ehlo_after_tls"
                stage_started = time.perf_counter()
                code, _response = server.ehlo()
                log_stage_success(
                    trace_id=trace_id,
                    channel=channel,
                    attempt=attempt,
                    stage=stage,
                    started=stage_started,
                    details=f"port={port} smtp_code={code} extension_count={len(server.esmtp_features)}",
                )
            else:
                logger.warning(
                    "SMTP_DIAG stage_result trace_id=%s channel=%s attempt=%d "
                    "stage=tls_handshake result=skipped port=%d reason=starttls_not_advertised",
                    trace_id,
                    channel,
                    attempt,
                    port,
                )

        stage = "authentication"
        stage_started = time.perf_counter()
        code, _response = server.login(config.sender_email, smtp_password)
        log_stage_success(
            trace_id=trace_id,
            channel=channel,
            attempt=attempt,
            stage=stage,
            started=stage_started,
            details=f"port={port} smtp_code={code}",
        )

        server_sock = getattr(server, "sock", None)
        if data_timeout and server_sock is not None:
            server_sock.settimeout(data_timeout)

        stage = "message_data"
        stage_started = time.perf_counter()
        refused = server.send_message(msg, to_addrs=recipients)
        if refused:
            raise smtplib.SMTPRecipientsRefused(refused)
        accepted = True
        log_stage_success(
            trace_id=trace_id,
            channel=channel,
            attempt=attempt,
            stage=stage,
            started=stage_started,
            details="smtp_code=250_assumed refused_count=0",
        )
    except Exception as exc:
        log_stage_failure(
            exc,
            trace_id=trace_id,
            channel=channel,
            attempt=attempt,
            stage=stage,
            started=stage_started,
            port=port,
        )
        if route_intercepted:
            try:
                exc.proxy_intercepted = True
            except AttributeError:
                pass
        raise
    finally:
        if server is not None:
            quit_started = time.perf_counter()
            try:
                code, _response = server.quit()
                log_stage_success(
                    trace_id=trace_id,
                    channel=channel,
                    attempt=attempt,
                    stage="quit",
                    started=quit_started,
                    details=f"port={port} smtp_code={code} post_accept={str(accepted).lower()}",
                )
            except Exception as exc:
                logger.warning(
                    "SMTP_DIAG quit_result trace_id=%s channel=%s attempt=%d result=failed "
                    "elapsed_ms=%d port=%d exception=%s post_accept=%s",
                    trace_id,
                    channel,
                    attempt,
                    elapsed_ms(quit_started),
                    port,
                    type(exc).__name__,
                    str(accepted).lower(),
                )
                try:
                    server.close()
                except Exception:
                    pass
