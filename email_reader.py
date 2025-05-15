from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import os
import logging
import base64
from semantic_kernel.functions import kernel_function
import tempfile
import mimetypes

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("email_reader")

# Load environment variables
load_dotenv()

class EmailReaderPlugin:
    def __init__(self):
        self.client = EmailClient()

    @kernel_function(
        description="Fetch new (unread) emails from Gmail inbox.",
        name="fetch_new_emails"
    )
    async def fetch_new_emails(self, limit: int = 10) -> list:
        """
        Fetch new emails and return a list of email data.
        Args:
            limit (int): Maximum number of emails to fetch (default: 10).
        Returns:
            list: List of dictionaries with email details (id, subject, from, received, body, threadId, attachments).
        """
        return self.client.fetch_new_emails(limit)

class EmailClient:
    def __init__(self):
        self.email_address = os.getenv("EMAIL_ADDRESS")
        self.service = None
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

            self.service = build('gmail', 'v1', credentials=creds)
            logger.info(f"Authenticated to {self.email_address}")
        except Exception as e:
            logger.error(f"Failed to initialize Gmail service: {str(e)}")
            raise

    def _decode_body(self, data):
        """Decode base64-encoded email body."""
        try:
            if not data:
                return ""
            decoded = base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
            return decoded.strip()
        except Exception as e:
            logger.error(f"Error decoding body: {str(e)}")
            return ""

    def _extract_body(self, payload):
        """Recursively extract email body from payload."""
        body = ""
        mime_type = payload.get('mimeType', '')

        if 'body' in payload and 'data' in payload['body']:
            body = self._decode_body(payload['body']['data'])
            if body:
                return body

        if 'parts' in payload:
            for part in payload['parts']:
                if part.get('mimeType') == 'text/plain':
                    body = self._decode_body(part['body'].get('data', ''))
                    if body:
                        return body
                elif part.get('mimeType') == 'text/html':
                    html_body = self._decode_body(part['body'].get('data', ''))
                    if html_body:
                        body = html_body
                elif 'parts' in part:
                    nested_body = self._extract_body(part)
                    if nested_body:
                        body = nested_body
                        if part.get('mimeType') == 'text/plain':
                            return body

        return body

    def _extract_attachments(self, msg, message_id):
        """Extract all attachments and save to temporary files."""
        attachments = []
        if 'parts' in msg['payload']:
            for part in msg['payload']['parts']:
                if part.get('filename') and 'body' in part and part['body'].get('attachmentId'):
                    attachment_id = part['body']['attachmentId']
                    filename = part['filename']
                    mime_type = part.get('mimeType', 'application/octet-stream')
                    try:
                        attachment = self.service.users().messages().attachments().get(
                            userId='me', messageId=message_id, id=attachment_id
                        ).execute()
                        data = base64.urlsafe_b64decode(attachment['data'])
                        # Determine file extension from MIME type or filename
                        extension = mimetypes.guess_extension(mime_type) or os.path.splitext(filename)[1] or '.bin'
                        # Save to temporary file
                        with tempfile.NamedTemporaryFile(delete=False, suffix=extension, prefix='email_attachment_') as temp_file:
                            temp_file.write(data)
                            temp_path = temp_file.name
                        attachments.append({
                            'filename': filename,
                            'mimeType': mime_type,
                            'path': temp_path
                        })
                        logger.info(f"Saved attachment {filename} (MIME: {mime_type}) to {temp_path}")
                    except Exception as e:
                        logger.error(f"Error fetching attachment {filename}: {str(e)}")
        return attachments

    def fetch_new_emails(self, limit=10):
        """Fetch new (unread) emails from inbox."""
        try:
            results = self.service.users().messages().list(
                userId='me',
                labelIds=['INBOX'],
                q='is:unread',
                maxResults=limit
            ).execute()
            messages = results.get('messages', [])
            emails = []

            for message in messages:
                msg = self.service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()

                headers = msg['payload']['headers']
                subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), '')
                from_addr = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
                received = msg.get('internalDate', '')

                body = self._extract_body(msg['payload'])
                attachments = self._extract_attachments(msg, message['id'])

                logger.info(f"Extracted email ID={message['id']}, Subject={subject}, Body={body[:100]}..., Attachments={len(attachments)}")

                emails.append({
                    "id": message['id'],
                    "subject": subject,
                    "from": from_addr,
                    "received": received,
                    "body": body,
                    "threadId": msg.get('threadId', message['id']),
                    "attachments": attachments
                })

                try:
                    self.service.users().messages().modify(
                        userId='me',
                        id=message['id'],
                        body={'removeLabelIds': ['UNREAD']}
                    ).execute()
                    logger.info(f"Marked email ID={message['id']} as read")
                except HttpError as e:
                    logger.warning(f"Failed to mark email {message['id']} as read: {str(e)}")

            logger.info(f"Fetched {len(emails)} new emails")
            return emails
        except HttpError as e:
            logger.error(f"Error fetching emails: {str(e)}")
            return []