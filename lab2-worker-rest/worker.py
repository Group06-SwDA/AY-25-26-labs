import logging
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MZINGA_URL = os.environ["MZINGA_URL"]
MZINGA_EMAIL = os.environ["MZINGA_EMAIL"]
MZINGA_PASSWORD = os.environ["MZINGA_PASSWORD"]
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL_SECONDS", "5"))
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
EMAIL_FROM = os.getenv("EMAIL_FROM", "worker@mzinga.io")


def login() -> str:
	response = requests.post(
		f"{MZINGA_URL}/api/users/login",
		json={"email": MZINGA_EMAIL, "password": MZINGA_PASSWORD},
		timeout=20,
	)
	response.raise_for_status()
	token = response.json().get("token")
	if not token:
		raise RuntimeError("Missing token in login response")
	log.info("Authenticated with MZinga API")
	return token


def auth_headers(token: str) -> dict:
	return {"Authorization": f"Bearer {token}"}


def call_with_reauth(token: str, method: str, url: str, **kwargs):
	response = requests.request(method, url, headers=auth_headers(token), timeout=20, **kwargs)
	if response.status_code == 401:
		log.warning("Token expired, re-authenticating")
		token = login()
		response = requests.request(method, url, headers=auth_headers(token), timeout=20, **kwargs)
	response.raise_for_status()
	return token, response


def fetch_pending(token: str):
	token, response = call_with_reauth(
		token,
		"GET",
		f"{MZINGA_URL}/api/communications",
		params={"where[status][equals]": "pending", "depth": 1},
	)
	return token, response.json().get("docs", [])


def update_status(token: str, doc_id: str, status: str):
	token, _ = call_with_reauth(
		token,
		"PATCH",
		f"{MZINGA_URL}/api/communications/{doc_id}",
		json={"status": status},
	)
	return token


def slate_to_html(nodes):
	html = ""
	for node in nodes or []:
		node_type = node.get("type")
		if node_type == "paragraph":
			html += f"<p>{slate_to_html(node.get('children', []))}</p>"
		elif node_type == "h1":
			html += f"<h1>{slate_to_html(node.get('children', []))}</h1>"
		elif node_type == "h2":
			html += f"<h2>{slate_to_html(node.get('children', []))}</h2>"
		elif node_type == "ul":
			html += f"<ul>{slate_to_html(node.get('children', []))}</ul>"
		elif node_type == "li":
			html += f"<li>{slate_to_html(node.get('children', []))}</li>"
		elif node_type == "link":
			url = node.get("url", "#")
			html += f'<a href="{url}">{slate_to_html(node.get("children", []))}</a>'
		elif "text" in node:
			text = node["text"]
			if node.get("bold"):
				text = f"<strong>{text}</strong>"
			if node.get("italic"):
				text = f"<em>{text}</em>"
			html += text
		else:
			html += slate_to_html(node.get("children", []))
	return html


def extract_emails(relationship_list):
	emails = []
	for relation in relationship_list or []:
		value = relation.get("value") or {}
		if isinstance(value, dict) and value.get("email"):
			emails.append(value["email"])
	return emails


def send_email(to_addresses, subject, html, cc_addresses=None, bcc_addresses=None):
	cc_addresses = cc_addresses or []
	bcc_addresses = bcc_addresses or []
	msg = MIMEMultipart("alternative")
	msg["Subject"] = subject
	msg["From"] = EMAIL_FROM
	msg["To"] = ", ".join(to_addresses)
	if cc_addresses:
		msg["Cc"] = ", ".join(cc_addresses)
	msg.attach(MIMEText(html, "html"))

	all_recipients = to_addresses + cc_addresses + bcc_addresses
	with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as smtp:
		smtp.sendmail(EMAIL_FROM, all_recipients, msg.as_string())


def process_doc(token: str, doc: dict):
	doc_id = doc["id"]
	log.info("Processing communication %s", doc_id)
	token = update_status(token, doc_id, "processing")

	try:
		to_emails = extract_emails(doc.get("tos"))
		if not to_emails:
			raise ValueError("No valid 'to' email addresses found")

		cc_emails = extract_emails(doc.get("ccs"))
		bcc_emails = extract_emails(doc.get("bccs"))
		html = slate_to_html(doc.get("body") or [])
		send_email(to_emails, doc.get("subject", "(no subject)"), html, cc_emails, bcc_emails)

		token = update_status(token, doc_id, "sent")
		log.info("Communication %s sent successfully", doc_id)
	except Exception as exc:
		log.error("Failed to process communication %s: %s", doc_id, exc)
		token = update_status(token, doc_id, "failed")

	return token


def poll_forever():
	token = login()
	log.info("Worker started. Poll interval: %ss", POLL_INTERVAL)

	while True:
		try:
			token, docs = fetch_pending(token)
			if not docs:
				time.sleep(POLL_INTERVAL)
				continue

			for doc in docs:
				token = process_doc(token, doc)
		except requests.RequestException as exc:
			log.error("HTTP error while polling: %s", exc)
			time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
	poll_forever()
