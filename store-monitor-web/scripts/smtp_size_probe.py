"""Exercise production MIME rendering and SMTP DATA with large local messages.

This command never contacts an external SMTP server. It starts an authenticated
SMTP sink on 127.0.0.1, sends realistic product tables through the production
``smtp_send`` function, and prints build/send timings plus exact byte counts.
"""

from __future__ import annotations

import argparse
import logging
import socketserver
import sys
import threading
import time
from dataclasses import dataclass
from email.mime.text import MIMEText
from pathlib import Path
from types import SimpleNamespace


PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from scheduler_notifications import render_product_table, smtp_send  # noqa: E402


@dataclass
class ReceivedMessage:
    byte_count: int
    accepted: bool


class LocalSMTPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, reject_over_bytes: int | None, drop_over_bytes: int | None):
        super().__init__(("127.0.0.1", 0), LocalSMTPHandler)
        self.reject_over_bytes = reject_over_bytes
        self.drop_over_bytes = drop_over_bytes
        self.messages: list[ReceivedMessage] = []
        self.messages_lock = threading.Lock()

    def record_message(self, byte_count: int, accepted: bool) -> None:
        with self.messages_lock:
            self.messages.append(ReceivedMessage(byte_count, accepted))


class LocalSMTPHandler(socketserver.StreamRequestHandler):
    server: LocalSMTPServer

    def _reply(self, value: bytes) -> None:
        self.wfile.write(value + b"\r\n")
        self.wfile.flush()

    def handle(self) -> None:
        self._reply(b"220 localhost local-size-probe")
        while True:
            line = self.rfile.readline(65537)
            if not line:
                return
            command = line.decode("ascii", errors="replace").strip()
            verb = command.split(" ", 1)[0].upper()

            if verb in {"EHLO", "HELO"}:
                self._reply(b"250-localhost")
                self._reply(b"250-AUTH PLAIN LOGIN")
                self._reply(b"250 HELP")
            elif verb == "AUTH":
                self._reply(b"235 2.7.0 authentication successful")
            elif verb in {"MAIL", "RCPT", "RSET", "NOOP"}:
                self._reply(b"250 2.0.0 ok")
            elif verb == "DATA":
                self._reply(b"354 end data with <CR><LF>.<CR><LF>")
                byte_count = 0
                while True:
                    data_line = self.rfile.readline(65537)
                    if not data_line or data_line == b".\r\n":
                        break
                    byte_count += len(data_line)
                    if (
                        self.server.drop_over_bytes is not None
                        and byte_count > self.server.drop_over_bytes
                    ):
                        self.server.record_message(byte_count, False)
                        return

                threshold = self.server.reject_over_bytes
                accepted = threshold is None or byte_count <= threshold
                self.server.record_message(byte_count, accepted)
                if accepted:
                    self._reply(b"250 2.0.0 queued locally")
                else:
                    self._reply(b"552 5.3.4 message too large (local simulation)")
            elif verb == "QUIT":
                self._reply(b"221 2.0.0 bye")
                return
            else:
                self._reply(b"502 5.5.1 command not implemented")


def build_message(product_count: int) -> MIMEText:
    products = [
        {
            "name": f"模拟商品 {index:06d} - oversized email probe",
            "link": f"https://www.amazon.example.invalid/dp/TEST{index:010d}",
        }
        for index in range(product_count)
    ]
    body = "".join(
        [
            "<div style='font-family:sans-serif;padding:20px;'>",
            "<h2>本地超大型邮件测试</h2>",
            f"<p>当前全部商品链接 ({product_count})</p>",
            render_product_table(
                products,
                "#eff6ff",
                "#111111",
                "#2563eb",
                max_items=None,
            ),
            "</div>",
        ]
    )
    message = MIMEText(body, "html", "utf-8")
    message["Subject"] = f"[LOCAL TEST] {product_count} products"
    message["From"] = "sender@example.invalid"
    message["To"] = "recipient@example.invalid"
    return message


def parse_counts(value: str) -> list[int]:
    counts = []
    for item in value.split(","):
        count = int(item.strip())
        if count <= 0:
            raise argparse.ArgumentTypeError("counts must be positive integers")
        counts.append(count)
    return counts


def run_probe(
    counts: list[int],
    reject_over_bytes: int | None,
    drop_over_bytes: int | None,
) -> int:
    server = LocalSMTPServer(reject_over_bytes, drop_over_bytes)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    port = int(server.server_address[1])
    config = SimpleNamespace(
        label="local-size-probe",
        smtp_server="127.0.0.1",
        smtp_port=port,
        sender_email="sender@example.invalid",
    )

    print("LOCAL_ONLY server=127.0.0.1 port=%d external_email=false" % port)
    if reject_over_bytes is not None:
        print("LOCAL_REJECT_THRESHOLD bytes=%d" % reject_over_bytes)
    if drop_over_bytes is not None:
        print("LOCAL_DROP_THRESHOLD bytes=%d" % drop_over_bytes)
    print("products,mime_bytes,received_bytes,build_ms,send_ms,result,exception")

    unexpected = 0
    try:
        for product_count in counts:
            build_started = time.perf_counter()
            message = build_message(product_count)
            mime_bytes = len(message.as_bytes())
            build_ms = round((time.perf_counter() - build_started) * 1000)

            before_count = len(server.messages)
            send_started = time.perf_counter()
            exception_name = "none"
            try:
                smtp_send(
                    config,
                    "local-dummy-password",
                    message,
                    ["recipient@example.invalid"],
                    retry_attempts=0,
                )
                result = "accepted"
            except Exception as exc:
                exception_name = type(exc).__name__
                result = "rejected"
            send_ms = round((time.perf_counter() - send_started) * 1000)

            received = server.messages[-1] if len(server.messages) > before_count else None
            received_bytes = received.byte_count if received else 0
            expected_acceptance = (
                (reject_over_bytes is None or received_bytes <= reject_over_bytes)
                and (drop_over_bytes is None or received_bytes <= drop_over_bytes)
            )
            actual_acceptance = result == "accepted"
            if expected_acceptance != actual_acceptance:
                unexpected += 1
                result = "unexpected_" + result

            print(
                f"{product_count},{mime_bytes},{received_bytes},{build_ms},{send_ms},"
                f"{result},{exception_name}"
            )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=5)

    return 1 if unexpected else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Send realistic large reports to a loopback-only SMTP sink."
    )
    parser.add_argument(
        "--counts",
        type=parse_counts,
        default=parse_counts("1,100,1000,10000,50000"),
        help="comma-separated product counts (default: 1,100,1000,10000,50000)",
    )
    parser.add_argument(
        "--reject-over-mib",
        type=float,
        default=None,
        help="simulate SMTP 552 after DATA when received bytes exceed this MiB value",
    )
    parser.add_argument(
        "--drop-over-mib",
        type=float,
        default=None,
        help="simulate an abrupt connection loss during DATA after this MiB value",
    )
    args = parser.parse_args()
    if args.reject_over_mib is not None and args.drop_over_mib is not None:
        parser.error("--reject-over-mib and --drop-over-mib are mutually exclusive")
    reject_over_bytes = (
        None
        if args.reject_over_mib is None
        else max(0, round(args.reject_over_mib * 1024 * 1024))
    )
    drop_over_bytes = (
        None
        if args.drop_over_mib is None
        else max(0, round(args.drop_over_mib * 1024 * 1024))
    )

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    return run_probe(args.counts, reject_over_bytes, drop_over_bytes)


if __name__ == "__main__":
    raise SystemExit(main())
