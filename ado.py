from azure.devops.connection import Connection
from msrest.authentication import BasicAuthentication
import os
from dotenv import load_dotenv
import logging
from semantic_kernel.functions import kernel_function
import datetime
import tempfile
import base64

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ADOPlugin:
    def __init__(self):
        self.client = ADOClient()

    @kernel_function(
        description="Fetch all work items in the Azure DevOps project.",
        name="get_all_work_items"
    )
    async def get_all_work_items(self) -> list:
        """
        Fetch all ADO work items.
        Returns:
            list: List of work items with id, title, status, created, updated.
        """
        return self.client.get_all_work_items()

    @kernel_function(
        description="Create a new work item in Azure DevOps with optional email and image attachments.",
        name="create_ticket"
    )
    async def create_ticket(self, title: str, description: str, email_content: str = None, attachments: list = None) -> dict:
        """
        Create an ADO ticket with optional email and image attachments.
        Args:
            title (str): Ticket title.
            description (str): Ticket description.
            email_content (str): Raw email content to attach as .eml (optional).
            attachments (list): List of attachment metadata (filename, path, mimeType) (optional).
        Returns:
            dict: Ticket details {id, url} or None if failed.
        """
        return self.client.create_ticket(title, description, email_content, attachments)

    @kernel_function(
        description="Update an Azure DevOps ticket with status and comment.",
        name="update_ticket"
    )
    async def update_ticket(self, ticket_id: int, status: str, comment: str) -> dict:
        """
        Update an ADO ticket.
        Args:
            ticket_id (int): Ticket ID.
            status (str): New status (To Do, Doing, Done).
            comment (str): Update comment.
        Returns:
            dict: Updated ticket details {id, status, comment} or None if failed.
        """
        return self.client.update_ticket(ticket_id, status, comment)

    @kernel_function(
        description="Fetch updates for an Azure DevOps ticket.",
        name="get_ticket_updates"
    )
    async def get_ticket_updates(self, ticket_id: int) -> list:
        """
        Fetch ticket updates.
        Args:
            ticket_id (int): Ticket ID.
        Returns:
            list: List of updates {comment, status, revision_id, attachments}.
        """
        return self.client.get_ticket_updates(ticket_id)

class ADOClient:
    def __init__(self):
        self.organization_url = os.getenv("ADO_ORGANIZATION_URL")
        self.personal_access_token = os.getenv("ADO_PERSONAL_ACCESS_TOKEN")
        self.project = os.getenv("ADO_PROJECT")
        self.connection = self._initialize_connection()
        self.client = self.connection.clients.get_work_item_tracking_client()
        logger.info("Initialized Azure DevOps client")

    def _initialize_connection(self):
        """Initialize connection to Azure DevOps."""
        try:
            credentials = BasicAuthentication("", self.personal_access_token)
            connection = Connection(base_url=self.organization_url, creds=credentials)
            return connection
        except Exception as e:
            logger.error(f"Failed to initialize ADO connection: {str(e)}")
            raise

    def create_ticket(self, title, description, email_content=None, attachments=None):
        """Create a new work item in Azure DevOps with optional email and image attachments."""
        try:
            document = [
                {
                    "op": "add",
                    "path": "/fields/System.Title",
                    "value": title
                },
                {
                    "op": "add",
                    "path": "/fields/System.Description",
                    "value": description
                },
                {
                    "op": "add",
                    "path": "/fields/System.WorkItemType",
                    "value": "Issue"
                }
            ]
            work_item = self.client.create_work_item(
                document=document,
                project=self.project,
                type="Issue"
            )
            ticket = {
                "id": work_item.id,
                "url": f"{self.organization_url}/{self.project}/_workitems/edit/{work_item.id}",
                "attachments": []
            }
            logger.info(f"Created ADO work item: ID={ticket['id']}, Title={title}")

            # Attach email if provided
            if email_content:
                attachment_url = self._upload_attachment(email_content, f"email_{work_item.id}.eml", is_eml=True)
                if attachment_url:
                    document = [
                        {
                            "op": "add",
                            "path": "/relations/-",
                            "value": {
                                "rel": "AttachedFile",
                                "url": attachment_url,
                                "attributes": {"comment": "Email associated with the ticket"}
                            }
                        }
                    ]
                    self.client.update_work_item(
                        document=document,
                        id=work_item.id,
                        project=self.project
                    )
                    ticket['attachments'].append({'filename': f"email_{work_item.id}.eml", 'url': attachment_url})
                    logger.info(f"Attached email to ADO work item: ID={work_item.id}")

            # Attach images if provided
            if attachments:
                for attachment in attachments:
                    attachment_url = self._upload_attachment(attachment['path'], attachment['filename'], is_eml=False)
                    if attachment_url:
                        document = [
                            {
                                "op": "add",
                                "path": "/relations/-",
                                "value": {
                                    "rel": "AttachedFile",
                                    "url": attachment_url,
                                    "attributes": {"comment": f"Image attachment: {attachment['filename']}"}
                                }
                            }
                        ]
                        self.client.update_work_item(
                            document=document,
                            id=work_item.id,
                            project=self.project
                        )
                        ticket['attachments'].append({'filename': attachment['filename'], 'url': attachment_url})
                        logger.info(f"Attached image {attachment['filename']} to ADO work item: ID={work_item.id}")

            return ticket
        except Exception as e:
            logger.error(f"Error creating ADO ticket: {str(e)}")
            return None

    def _upload_attachment(self, content, filename, is_eml=False):
        """Upload a file as an attachment to Azure DevOps."""
        temp_file_path = None
        try:
            if is_eml:
                # For .eml, content is a string
                with tempfile.NamedTemporaryFile(delete=False, suffix=".eml", mode='w', encoding='utf-8') as temp_file:
                    temp_file.write(content)
                    temp_file_path = temp_file.name
            else:
                # For images, content is a file path
                temp_file_path = content

            # Reopen the file in binary mode for uploading
            with open(temp_file_path, 'rb') as file:
                attachment = self.client.create_attachment(
                    upload_stream=file,
                    file_name=filename,
                    upload_type="Simple",
                    project=self.project
                )
                logger.info(f"Uploaded attachment: {filename}")
                return attachment.url
        except Exception as e:
            logger.error(f"Error uploading attachment {filename}: {str(e)}")
            return None
        finally:
            # Clean up the temporary file for .eml
            if is_eml and temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    logger.info(f"Deleted temporary file: {temp_file_path}")
                except Exception as e:
                    logger.error(f"Error deleting temporary file {temp_file_path}: {str(e)}")

    def get_all_work_items(self):
        """Fetch all work items in the project using WIQL."""
        try:
            wiql_query = f"""
            SELECT [System.Id], [System.Title], [System.State], [System.CreatedDate], [System.ChangedDate]
            FROM WorkItems
            WHERE [System.TeamProject] = '{self.project}'
            """
            query_result = self.connection.clients.get_work_item_tracking_client().query_by_wiql(
                wiql={"query": wiql_query}
            )
            work_items = []
            for wi in query_result.work_items:
                work_item = self.client.get_work_item(wi.id, project=self.project, expand="All")
                work_items.append({
                    "id": work_item.id,
                    "title": work_item.fields.get("System.Title", f"Ticket {work_item.id}"),
                    "status": work_item.fields.get("System.State", "New"),
                    "created": work_item.fields.get("System.CreatedDate", datetime.datetime.now().isoformat()),
                    "updated": work_item.fields.get("System.ChangedDate", datetime.datetime.now().isoformat())
                })
            logger.info(f"Fetched {len(work_items)} work items")
            return work_items
        except Exception as e:
            logger.error(f"Error fetching work items: {str(e)}")
            return []

    def get_ticket_updates(self, ticket_id):
        """Fetch all updates for a specific ticket."""
        try:
            updates = []
            work_item = self.client.get_work_item(ticket_id, project=self.project, expand="All")
            revisions = self.client.get_revisions(ticket_id, project=self.project)

            for revision in revisions:
                fields = revision.fields if hasattr(revision, 'fields') else {}
                comment = ""
                if 'System.History' in fields:
                    history = fields['System.History']
                    comment = history.get('newValue', '') if isinstance(history, dict) else (history or '')
                
                status = work_item.fields.get("System.State", "To Do")

                updates.append({
                    "comment": comment,
                    "status": status,
                    "revision_id": revision.rev,
                    "attachments": []  # Attachments handled in create_ticket
                })

            logger.info(f"Fetched {len(updates)} updates for ticket ID={ticket_id}")
            return updates
        except Exception as e:
            logger.error(f"Error fetching updates for ticket ID={ticket_id}: {str(e)}")
            return []

    def update_ticket(self, ticket_id, status, comment):
        """Update ticket status and add a comment."""
        try:
            valid_states = ["To Do", "Doing", "Done"]
            if status not in valid_states:
                logger.warning(f"Invalid status: {status}. Using 'To Do'.")
                status = "To Do"

            document = [
                {
                    "op": "add",
                    "path": "/fields/System.State",
                    "value": status
                },
                {
                    "op": "add",
                    "path": "/fields/System.History",
                    "value": comment
                }
            ]
            updated_work_item = self.client.update_work_item(
                document=document,
                id=ticket_id,
                project=self.project
            )
            logger.info(f"Updated ADO ticket ID={ticket_id} with status={status}, comment={comment}")
            return {
                "id": updated_work_item.id,
                "status": status,
                "comment": comment
            }
        except Exception as e:
            logger.error(f"Error updating ADO ticket ID={ticket_id}: {str(e)}")
            return None