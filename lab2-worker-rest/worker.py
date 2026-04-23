import html
import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Iterable

import requests
from dotenv import load_dotenv


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("lab2-worker")

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.getenv("ADMIN_EMAIL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")
REQUEST_TIMEOUT_SECONDS = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))

if not ADMIN_EMAIL:
    raise RuntimeError("ADMIN_EMAIL is required")
if not ADMIN_PASSWORD:
    raise RuntimeError("ADMIN_PASSWORD is required")


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
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()

        payload = response.json()
        token = payload.get("token")
        if not isinstance(token, str) or not token.strip():
            raise RuntimeError("Login succeeded but no JWT token was returned")

        self.token = token
        self.session.headers.update({"Authorization": f"JWT {self.token}"})
        logger.info("Authenticated against Payload API")

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        if not self.token:
            self.authenticate()

        url = f"{self.base_url}{path}"
        response = self.session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)

        if response.status_code == 401:
            logger.warning("Received 401 from Payload API, re-authenticating")
            self.authenticate()
            response = self.session.request(method, url, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs)

        response.raise_for_status()
        return response

    def get_pending_documents(self) -> list[dict[str, Any]]:
        response = self.request(
            "GET",
            "/api/communications",
            params={
                "where[status][equals]": "pending",
                "depth": 1,
                "limit": 100,
                "sort": "createdAt",
            },
        )
        payload = response.json()
        docs = payload.get("docs", [])
        if isinstance(docs, list):
            return [doc for doc in docs if isinstance(doc, dict)]
        return []

    def update_status(self, document_id: str, status: str, error_message: str | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"status": status}
        if error_message is not None:
            body["workerError"] = error_message[:1000]

        response = self.request(
            "PATCH",
            f"/api/communications/{document_id}",
            json=body,
        )
        payload = response.json()
        if isinstance(payload, dict) and "doc" in payload and isinstance(payload["doc"], dict):
            return payload["doc"]
        return payload if isinstance(payload, dict) else {}


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
        normalized = value.strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def extract_emails(relations: Any) -> list[str]:
    emails: list[str] = []
    if not isinstance(relations, list):
        return emails

    for item in relations:
        if not isinstance(item, dict):
            continue

        value = item.get("value")
        if isinstance(value, dict):
            email_address = value.get("email")
            if isinstance(email_address, str):
                emails.append(email_address)
        elif isinstance(item.get("email"), str):
            emails.append(str(item["email"]))

    return dedupe_keep_order(emails)


def send_email(document: dict[str, Any]) -> None:
    to_emails = extract_emails(document.get("tos"))
    cc_emails = extract_emails(document.get("ccs"))
    bcc_emails = extract_emails(document.get("bccs"))

    if not to_emails and not cc_emails and not bcc_emails:
        raise ValueError("Communication has no resolvable recipients")

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

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
        smtp.sendmail(EMAIL_FROM, all_recipients, msg.as_string())


def main() -> None:
    client = PayloadClient(API_BASE_URL, ADMIN_EMAIL, ADMIN_PASSWORD)
    logger.info("Worker started. Polling %s every %s seconds.", API_BASE_URL, POLL_INTERVAL_SECONDS)

    while True:
        try:
            pending_documents = client.get_pending_documents()
        except Exception:
            logger.exception("Failed to fetch pending communications")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if not pending_documents:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        for document in pending_documents:
            document_id = document.get("id")
            if not isinstance(document_id, str) or not document_id:
                logger.warning("Skipping communication without a valid id: %r", document)
                continue

            try:
                claimed_document = client.update_status(document_id, "processing")
                logger.info("Claimed communication %s", document_id)
                working_document = claimed_document if claimed_document else document

                send_email(working_document)
                client.update_status(document_id, "sent")
                logger.info("Communication %s marked sent", document_id)
            except Exception as exc:
                logger.exception("Failed processing communication %s", document_id)
                try:
                    client.update_status(document_id, "failed", str(exc))
                except Exception:
                    logger.exception("Failed to mark communication %s as failed", document_id)


if __name__ == "__main__":
    main()
