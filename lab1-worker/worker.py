import html
import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Iterable

from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient, ReturnDocument


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("lab1-worker")

MONGODB_URI = os.getenv("MONGODB_URI")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")

if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI is required")


client = MongoClient(MONGODB_URI)
db = client.get_default_database()
communications = db["communications"]
users = db["users"]


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


def normalize_relation_ids(relations: Any) -> list[ObjectId]:
    ids: list[ObjectId] = []
    if not isinstance(relations, list):
        return ids

    for item in relations:
        if not isinstance(item, dict):
            continue
        if item.get("relationTo") != "users":
            continue

        value = item.get("value")
        if isinstance(value, ObjectId):
            ids.append(value)
            continue
        if isinstance(value, str):
            try:
                ids.append(ObjectId(value))
            except Exception:
                logger.warning("Skipping invalid ObjectId string: %s", value)

    return ids


def resolve_email_addresses(relations: Any) -> list[str]:
    user_ids = normalize_relation_ids(relations)
    if not user_ids:
        return []

    cursor = users.find({"_id": {"$in": user_ids}}, {"email": 1})
    emails = []
    for user_doc in cursor:
        email_address = user_doc.get("email")
        if isinstance(email_address, str) and email_address.strip():
            emails.append(email_address.strip())

    return emails


def dedupe_keep_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def send_email(document: dict[str, Any]) -> None:
    to_emails = dedupe_keep_order(resolve_email_addresses(document.get("tos")))
    cc_emails = dedupe_keep_order(resolve_email_addresses(document.get("ccs")))
    bcc_emails = dedupe_keep_order(resolve_email_addresses(document.get("bccs")))

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


def claim_next_pending() -> dict[str, Any] | None:
    return communications.find_one_and_update(
        {"status": "pending"},
        {"$set": {"status": "processing", "workerPickedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}},
        sort=[("createdAt", 1), ("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_status(document_id: ObjectId, status: str, error_message: str | None = None) -> None:
    update: dict[str, Any] = {"status": status}
    if error_message:
        update["workerError"] = error_message[:1000]
    else:
        update["workerError"] = None

    communications.update_one({"_id": document_id}, {"$set": update})


def main() -> None:
    logger.info("Worker started. Polling every %s seconds.", POLL_INTERVAL_SECONDS)

    while True:
        document = claim_next_pending()
        if not document:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        doc_id = document.get("_id")
        logger.info("Claimed communication %s", doc_id)

        try:
            send_email(document)
            mark_status(doc_id, "sent")
            logger.info("Communication %s marked sent", doc_id)
        except Exception as exc:
            logger.exception("Failed processing communication %s", doc_id)
            mark_status(doc_id, "failed", str(exc))


if __name__ == "__main__":
    main()
