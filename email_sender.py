from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import os
import logging
import base64
from semantic_kernel.functions import kernel_function
import email.mime.text
import email.mime.multipart
import email.mime.base
from email import encoders
import time
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("email_sender")

# Load environment variables
load_dotenv()

class EmailSenderPlugin:
    def __init__(self):
        self.client = EmailSenderClient()
        logger.info("Initialized Gmail send service")

    @kernel_function(
        description="Send an email reply to a recipient with optional attachments and remediation.",
        name="send_reply"
    )
    async def send_reply(self, to: str, subject: str, body: str, thread_id: str, message_id: str, attachments: list = None, remediation: str = None) -> dict:
        """
        Send an email reply.
        Args:
            to (str): Recipient email address.
            subject (str): Email subject.
            body (str): Email body.
            thread_id (str): Thread ID for reply.
            message_id (str): Message ID for In-Reply-To header.
            attachments (list): List of attachment metadata (filename, path, mimeType) (optional).
            remediation (str): Remediation steps to include in the body (optional).
        Returns:
            dict: {'message_id': str} or None if failed or skipped due to deduplication.
        """
        return self.client.send_reply(to, subject, body, thread_id, message_id, attachments, remediation)

class EmailSenderClient:
    def __init__(self):
        self.email_address = os.getenv("EMAIL_ADDRESS")
        self.service = None
        self.sent_replies = defaultdict(float)  # In-memory cache: {thread_id: timestamp}
        self._initialize_service()

    def _initialize_service(self):
        """Initialize Gmail API service."""
        try:
            SCOPES = ['https://www.googleapis.com/auth/gmail.modify']
            creds = None
            token_path = 'token.json'

            if os.path.exists(token_path):
                creds = Credentials.from_authorized_user_file(token_path, SCOPES)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        'credentials.json', SCOPES)
                    creds = flow.run_local_server(port=0)
                with open(token_path, 'w') as token:
                    token.write(creds.to_json())

            # Configure null cache to suppress oauth2client warning
            from googleapiclient.discovery_cache.base import Cache
            class NullCache(Cache):
                def get(self, url): pass
                def set(self, url, content): pass
            discovery_cache = NullCache()

            self.service = build('gmail', 'v1', credentials=creds, cache=discovery_cache)
            logger.info(f"Authenticated to {self.email_address} for sending")
        except Exception as e:
            logger.error(f"Failed to initialize Gmail send service: {str(e)}")
            raise

    def send_reply(self, to, subject, body, thread_id, message_id, attachments=None, remediation=None):
        """Send an email reply, skipping duplicates within a time window."""
        try:
            # Validate thread_id and message_id
            if not thread_id or len(thread_id) < 10:
                logger.warning(f"Invalid or missing thread_id: {thread_id}")
            if not message_id or len(message_id) < 10:
                logger.warning(f"Invalid or missing message_id: {message_id}")

            # Check for duplicate reply
            current_time = time.time()
            dedup_key = f"{thread_id}:{message_id}"
            last_sent_time = self.sent_replies.get(dedup_key, 0)
            dedup_window = 300  # 5 minutes in seconds

            if current_time - last_sent_time < dedup_window:
                logger.info(f"Skipping duplicate reply to {to} for thread {thread_id}, last sent {(current_time - last_sent_time):.2f}s ago")
                return {'message_id': None, 'status': 'skipped_duplicate'}

            logger.info(f"Preparing reply to {to} with thread_id={thread_id}, message_id={message_id}")

            message = email.mime.multipart.MIMEMultipart()
            message['to'] = to
            # Ensure subject starts with "Re:" (case-insensitive)
            if not subject.lower().startswith('re:'):
                subject = f"Re: {subject}"
            message['subject'] = subject
            message['from'] = self.email_address
            message['In-Reply-To'] = message_id
            message['References'] = message_id

            # Append remediation if provided
            if remediation:
                body += f"\n\nWhile I get back to you, you can try these steps:\n{remediation}"

            # Add body
            message.attach(email.mime.text.MIMEText(body, 'plain'))

            # Add attachments
            if attachments:
                for attachment in attachments:
                    try:
                        with open(attachment['path'], 'rb') as f:
                            part = email.mime.base.MIMEBase(*attachment['mimeType'].split('/'))
                            part.set_payload(f.read())
                        encoders.encode_base64(part)
                        part.add_header(
                            'Content-Disposition',
                            f'attachment; filename={attachment["filename"]}'
                        )
                        message.attach(part)
                        logger.info(f"Attached {attachment['filename']} to reply")
                    except Exception as e:
                        logger.warning(f"Skipping attachment {attachment['filename']}: {str(e)}")
                        continue

            # Encode message
            raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
            message_data = {
                'raw': raw_message,
                'threadId': thread_id
            }

            sent_message = self.service.users().messages().send(
                userId='me',
                body=message_data
            ).execute()

            # Update deduplication cache
            self.sent_replies[dedup_key] = current_time
            # Clean up old entries (older than dedup_window)
            for key in list(self.sent_replies.keys()):
                if current_time - self.sent_replies[key] > dedup_window:
                    del self.sent_replies[key]

            logger.info(f"Sent reply to {to} for thread {thread_id}: {sent_message['id']}")
            return {'message_id': sent_message['id']}

        except Exception as e:
            logger.error(f"Error sending reply to {to}: {str(e)}")
            raise  # Propagate the exception to ensure failure is reported correctly