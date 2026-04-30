import asyncio
import html
import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Iterable

import aio_pika
import requests
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("lab2-worker-events")

RABBITMQ_URL = os.environ["RABBITMQ_URL"]
ROUTING_KEY = "HOOKSURL_COMMUNICATIONS_AFTERCHANGE"
EXCHANGE_NAME = "mzinga_events_durable"
QUEUE_NAME = "communications-email-worker"
API_BASE_URL = os.getenv("MZINGA_URL") or os.getenv("API_BASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ["MZINGA_EMAIL"]
ADMIN_PASSWORD = os.environ["MZINGA_PASSWORD"]
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")

if not ADMIN_EMAIL:
    raise RuntimeError("MZINGA_EMAIL is required")
if not ADMIN_PASSWORD:
    raise RuntimeError("MZINGA_PASSWORD is required")


class PayloadClient:
    def __init__(self, base_url: str, email: str, password: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self.session = requests.Session()
        self.token: str | None = None

    def authenticate(self) -> None:
        response = self.session.post(
            f"{self.base_url}/api/users/login",
            json={"email": self.email, "password": self.password},
            timeout=10,
        )
        response.raise_for_status()

        payload = response.json()
        self.token = payload["token"]
        self.session.headers.update({"Authorization": f"Bearer {self.token}"})
        logger.info("Authenticated against Payload API")

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}{path}"
        response = self.session.request(method, url, timeout=10, **kwargs)

        if response.status_code == 401:
            logger.warning("Received 401 from Payload API, re-authenticating")
            self.authenticate()
            response = self.session.request(method, url, timeout=10, **kwargs)

        response.raise_for_status()
        return response

    def fetch_doc(self, doc_id: str) -> dict[str, Any]:
        response = self.request(
            "GET",
            f"/api/communications/{doc_id}",
            params={"depth": 1},
        )
        return response.json()

    def update_status(self, document_id: str, status: str) -> dict[str, Any]:
        response = self.request(
            "PATCH",
            f"/api/communications/{document_id}",
            json={"status": status},
        )
        return response.json()


def get_child_html(node: dict[str, Any]) -> str:
    children = node.get("children", [])
    return "".join(render_node(child) for child in children)


def render_leaf(node: dict[str, Any]) -> str:
    text = html.escape(str(node.get("text", "")))
    if node.get("bold"):
        text = f"<strong>{text}</strong>"
    if node.get("italic"):
        text = f"<em>{text}</em>"
    return text


def render_node(node: Any) -> str:
    if not isinstance(node, dict):
        return ""

    if "text" in node:
        return render_leaf(node)

    node_type = node.get("type")
    children_html = get_child_html(node)

    if node_type == "paragraph":
        return f"<p>{children_html}</p>"
    if node_type == "h1":
        return f"<h1>{children_html}</h1>"
    if node_type == "h2":
        return f"<h2>{children_html}</h2>"
    if node_type == "ul":
        return f"<ul>{children_html}</ul>"
    if node_type == "li":
        return f"<li>{children_html}</li>"
    if node_type == "link":
        url = html.escape(str(node.get("url", "#")), quote=True)
        return f'<a href="{url}">{children_html}</a>'

    return children_html


def slate_to_html(body: Any) -> str:
    if not isinstance(body, list):
        return ""
    return "".join(render_node(node) for node in body)


def dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        value = value.strip()
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def extract_emails(relations: Any) -> list[str]:
    emails: list[str] = []

    if not isinstance(relations, list):
        return emails

    for item in relations:
        if not isinstance(item, dict):
            continue

        value = item.get("value")
        if isinstance(value, dict) and isinstance(value.get("email"), str):
            emails.append(value["email"])
        elif isinstance(item.get("email"), str):
            emails.append(item["email"])

    return dedupe_keep_order(emails)


def send_email(document: dict[str, Any]) -> None:
    to_emails = extract_emails(document.get("tos"))
    cc_emails = extract_emails(document.get("ccs"))
    bcc_emails = extract_emails(document.get("bccs"))

    if not to_emails and not cc_emails and not bcc_emails:
        raise ValueError("Communication has no recipients")

    subject = str(document.get("subject", ""))
    html_body = slate_to_html(document.get("body"))

    msg = MIMEMultipart("alternative")
    msg["From"] = EMAIL_FROM
    if to_emails:
        msg["To"] = ", ".join(to_emails)
    if cc_emails:
        msg["Cc"] = ", ".join(cc_emails)
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    all_recipients = to_emails + cc_emails + bcc_emails

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as smtp:
        smtp.sendmail(EMAIL_FROM, all_recipients, msg.as_string())


def process(client: PayloadClient, document: dict[str, Any]) -> None:
    document_id = str(document.get("id", "")).strip()
    if not document_id:
        logger.warning("Skipping communication without a valid id")
        return

    if document.get("status") in ("sent", "processing"):
        logger.info("Skipping %s — already %s", document_id, document["status"])
        return

    client.update_status(document_id, "processing")
    logger.info("Claimed communication %s", document_id)

    try:
        send_email(document)
        client.update_status(document_id, "sent")
        logger.info("Communication %s marked sent", document_id)
    except Exception:
        logger.exception("Failed processing communication %s", document_id)
        try:
            client.update_status(document_id, "failed")
        except Exception:
            logger.exception("Failed to mark communication %s as failed", document_id)


async def main() -> None:
    client = PayloadClient(API_BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD)
    client.authenticate()

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        exchange = await channel.declare_exchange(
            EXCHANGE_NAME, aio_pika.ExchangeType.TOPIC,
            durable=True, internal=True, auto_delete=False,
        )

        queue = await channel.declare_queue(QUEUE_NAME, durable=True)
        await queue.bind(exchange, routing_key=ROUTING_KEY)

        logger.info("Subscribed to %s with key %s. Waiting for messages.", EXCHANGE_NAME, ROUTING_KEY)

        async with queue.iterator() as messages:
            async for message in messages:
                async with message.process(requeue_on_timeout=True):
                    try:
                        body = json.loads(message.body.decode())
                        event_data = body.get("data", {})
                        operation = event_data.get("operation")
                        doc_id = (event_data.get("doc") or {}).get("id")

                        if not doc_id:
                            logger.warning("Message missing doc.id, skipping")
                            continue

                        # Filter out update operations: the worker's own PATCH
                        # triggers afterChange with operation="update", causing
                        # an infinite loop without this guard.
                        if operation != "create":
                            logger.debug("Ignoring operation=%s for %s", operation, doc_id)
                            continue

                        document = client.fetch_doc(doc_id)
                        process(client, document)

                    except Exception:
                        logger.exception("Unexpected error processing message")


if __name__ == "__main__":
    asyncio.run(main())
