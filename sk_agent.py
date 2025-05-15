import asyncio
import os
from datetime import datetime
import uuid
import logging
import json
from semantic_kernel import Kernel
from openai import AzureOpenAI
from bs4 import BeautifulSoup
from pymongo.collection import Collection
from pymongo.errors import DuplicateKeyError
from pymilvus import connections, Collection
from sentence_transformers import SentenceTransformer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SKAgent:
    def __init__(self, kernel, tickets_collection: Collection):
        self.kernel = kernel
        self.tickets_collection = tickets_collection
        self.client = AzureOpenAI(
            azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
            api_key=os.getenv("AZURE_OPENAI_KEY"),
            api_version="2023-05-15"
        )
        self.milvus_collection_name = "ticket_details"
        try:
            connections.connect(host="localhost", port="19530")
            self.milvus_collection = Collection(self.milvus_collection_name)
            self.milvus_collection.load()
            logger.info("Connected to Milvus successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Milvus: {str(e)}")
            raise
        try:
            self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
            logger.info("Initialized sentence-transformers model")
        except Exception as e:
            logger.error(f"Failed to initialize sentence-transformers: {str(e)}")
            raise
        logger.info("Initialized SKAgent with AzureOpenAI client")

    async def send_to_milvus(self, ticket: dict):
        """Send or update ticket details in Milvus for RAG."""
        try:
            ado_ticket_id = ticket.get("ado_ticket_id")
            if not ado_ticket_id:
                logger.warning("No ado_ticket_id provided, skipping Milvus operation")
                return

            ticket_title = ticket.get("ticket_title", "")
            ticket_description = ticket.get("ticket_description", "")
            updates = json.dumps(ticket.get("updates", []))

            # Generate embedding
            text_to_embed = f"{ticket_title} {ticket_description} {updates}"
            embedding = self.embedding_model.encode(text_to_embed).tolist()

            # Prepare data for upsert
            data = [
                [ado_ticket_id],
                [ticket_title],
                [ticket_description],
                [updates],
                [embedding]
            ]

            # Check if ticket exists in Milvus
            self.milvus_collection.load()
            results = self.milvus_collection.query(
                expr=f"ado_ticket_id == {ado_ticket_id}",
                output_fields=["ado_ticket_id"]
            )

            if results:
                # Update existing entry
                self.milvus_collection.delete(expr=f"ado_ticket_id == {ado_ticket_id}")
                logger.info(f"Deleted old Milvus entry for ticket ID={ado_ticket_id} before upsert")
                self.milvus_collection.insert(data)
                logger.info(f"Updated ticket ID={ado_ticket_id} in Milvus with new updates")
            else:
                # Insert new entry
                self.milvus_collection.insert(data)
                logger.info(f"Inserted new ticket ID={ado_ticket_id} in Milvus")

            # Update MongoDB with in_milvus flag
            self.tickets_collection.update_one(
                {"ado_ticket_id": ado_ticket_id},
                {"$set": {"in_milvus": True}}
            )

        except Exception as e:
            logger.error(f"Error in Milvus operation for ticket {ado_ticket_id}: {str(e)}")

    async def search_milvus_for_solution(self, ticket_title: str, ticket_description: str) -> tuple[bool, dict | None]:
        """Search Milvus for tickets similar to the given ticket_title and ticket_description."""
        try:
            # Use same text format as send_to_milvus (title + description, no updates for new ticket)
            text_to_embed = f"{ticket_title} {ticket_description}"
            embedding = self.embedding_model.encode(text_to_embed).tolist()
            search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
            results = self.milvus_collection.search(
                data=[embedding],
                anns_field="embedding",
                param=search_params,
                limit=3,
                output_fields=["ado_ticket_id", "ticket_title", "ticket_description", "updates"]
            )

            found_match = False
            threshold = 1.5  # Relaxed threshold for better match detection
            best_match = None
            min_distance = float('inf')

            for hits in results:
                for hit in hits:
                    logger.info(f"Search hit: ado_ticket_id={hit.entity.get('ado_ticket_id')}, distance={hit.distance}")
                    ticket_data = {
                        "ado_ticket_id": hit.entity.get('ado_ticket_id'),
                        "ticket_title": hit.entity.get('ticket_title'),
                        "ticket_description": hit.entity.get('ticket_description'),
                        "updates": hit.entity.get('updates')
                    }
                    if hit.distance < threshold and hit.distance < min_distance:
                        found_match = True
                        min_distance = hit.distance
                        best_match = ticket_data

            if found_match:
                logger.info(f"Found matching ticket: ado_ticket_id={best_match['ado_ticket_id']}, distance={min_distance}")
            else:
                logger.info("No matching ticket found in Milvus")
            return (found_match, best_match)

        except Exception as e:
            logger.error(f"Error searching Milvus: {str(e)}")
            return (False, None)

    async def generate_remediation_from_milvus(self, matching_ticket: dict) -> str:
        """Generate remediation steps based on a matching Milvus ticket."""
        try:
            ticket_id = matching_ticket.get("ado_ticket_id", "Unknown")
            ticket_content = (
                f"Title: {matching_ticket.get('ticket_title', '')}\n"
                f"Description: {matching_ticket.get('ticket_description', '')}\n"
                f"Updates: {matching_ticket.get('updates', '')}"
            )

            remediation_prompt = (
                "Generate 3-5 concise, actionable troubleshooting steps based on the provided IT ticket details. "
                "Format as a numbered list. "
                f"Ticket Details:\n{ticket_content}"
            )

            response = self.client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
                messages=[
                    {"role": "system", "content": "You are an IT support assistant."},
                    {"role": "user", "content": remediation_prompt}
                ],
                temperature=0.2,
                max_tokens=200
            )

            remediation = response.choices[0].message.content.strip()
            logger.info(f"Generated remediation for ticket ID={ticket_id}: {remediation}")
            return remediation

        except Exception as e:
            logger.error(f"Error generating remediation: {str(e)}")
            return ""

    async def analyze_intent(self, subject: str, body: str, attachments: list = None) -> dict:
        """Analyze email intent using Azure OpenAI."""
        try:
            if "<html>" in body.lower():
                body = BeautifulSoup(body, "html.parser").get_text(separator=" ").strip()
            else:
                body = body.strip()

            content = f"Subject: {subject}\nBody: {body}"
            if attachments:
                content += "\nAttachments: " + ", ".join(a['filename'] for a in attachments)

            prompt = (
                "Classify the email intent as one of: 'github_access_request', 'github_revoke_access', 'general_it_request', 'request_summary', or 'non_intent'. "
                "Extract details for actionable intents (repo_name, access_type, github_username). "
                "Return JSON: {'intent', 'ticket_description', 'actions', 'pending_actions', 'repo_name', 'access_type', 'github_username'}.\n"
                f"Email:\n{content}\n"
                "Examples:\n"
                "1. Subject: Request access\nBody: Grant read access to poc for testuser9731.\n"
                "   ```json\n{\"intent\": \"github_access_request\", \"ticket_description\": \"Grant read access to poc for testuser9731\", \"actions\": [{\"action\": \"grant_access\", \"repo_name\": \"poc\", \"access_type\": \"pull\", \"github_username\": \"testuser9731\"}], \"pending_actions\": false, \"repo_name\": \"poc\", \"access_type\": \"pull\", \"github_username\": \"testuser9731\"}\n```\n"
                "2. Subject: Thanks\nBody: Thanks for your help.\n"
                "   ```json\n{\"intent\": \"non_intent\", \"ticket_description\": \"Non-actionable email\", \"actions\": [], \"pending_actions\": false, \"repo_name\": \"unspecified\", \"access_type\": \"unspecified\", \"github_username\": \"unspecified\"}\n```"
            )

            response = self.client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
                messages=[
                    {"role": "system", "content": "You are an IT support assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=500
            )

            result = response.choices[0].message.content.strip()
            if result.startswith("```json") and result.endswith("```"):
                result = result[7:-3].strip()

            return json.loads(result)

        except Exception as e:
            logger.error(f"Error analyzing intent: {str(e)}")
            return {
                "intent": "error",
                "ticket_description": "Unable to determine intent",
                "actions": [],
                "pending_actions": False,
                "repo_name": "unspecified",
                "access_type": "unspecified",
                "github_username": "unspecified"
            }

    async def generate_summary_response(self, ticket_record: dict, user_request: str, request_source: str = "email") -> dict:
        """Generate a summary response for email or UI."""
        try:
            ticket_id = ticket_record.get("ado_ticket_id", "Unknown")
            subject = ticket_record.get("subject", "Unknown Request")
            email_chain = "\n".join(
                f"From: {e['from']}\nSubject: {e['subject']}\nTimestamp: {e['timestamp']}\nBody: {e['body']}"
                for e in ticket_record.get("email_chain", [])
            )
            updates = "\n".join(
                f"Status: {u['status']}\nComment: {u['comment']}\nTimestamp: {u['email_timestamp']}"
                for u in ticket_record.get("updates", [])
            )

            prompt = (
                f"Generate a {'conversational email' if request_source == 'email' else 'formal summary'} for ticket #{ticket_id}. "
                "Include ticket ID, summary of request, actions taken, and status. "
                f"User Request: {user_request}\nTicket ID: {ticket_id}\nSubject: {subject}\nEmail Chain:\n{email_chain}\nUpdates:\n{updates}"
            )

            response = self.client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
                messages=[
                    {"role": "system", "content": "You are an IT support assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=300
            )

            result = response.choices[0].message.content.strip()
            return {
                "summary_intent": "summary_provided",
                "email_response": result
            }

        except Exception as e:
            logger.error(f"Error generating summary: {str(e)}")
            return {
                "summary_intent": "error",
                "email_response": f"Error generating summary for ticket #{ticket_id}: {str(e)}."
            }

    async def analyze_ticket_update(self, ticket_id: int, updates: list, attachments: list = None) -> dict:
        """Analyze ADO ticket updates and generate email response."""
        try:
            update_text = "\n".join(
                f"Comment: {u['comment'] if u['comment'] else 'No comment'}, Status: {u['status']}"
                for u in updates
            )
            attachment_info = f"Attachments: {', '.join(a['filename'] for a in attachments)}" if attachments else ""

            prompt = (
                f"Generate a conversational email reply for ticket #{ticket_id}. "
                f"Include ticket ID, status, and update details. Mention attachments if present.\n"
                f"Updates:\n{update_text}\n{attachment_info}"
            )

            response = self.client.chat.completions.create(
                model=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
                messages=[
                    {"role": "system", "content": "You are an IT support assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                max_tokens=300
            )

            return {
                "update_intent": "action_completed",
                "email_response": response.choices[0].message.content.strip(),
                "remediation": ""
            }

        except Exception as e:
            logger.error(f"Error analyzing ticket update: {str(e)}")
            return {
                "update_intent": "error",
                "email_response": f"Error processing ticket #{ticket_id}: {str(e)}.",
                "remediation": ""
            }

    async def process_admin_request(self, ticket_id: int, admin_request: str) -> dict:
        """Process an admin's request for a ticket summary."""
        try:
            ticket_record = self.tickets_collection.find_one({"ado_ticket_id": ticket_id})
            if not ticket_record:
                return {
                    "summary_intent": "error",
                    "email_response": f"No ticket found for ID {ticket_id}."
                }

            return await self.generate_summary_response(ticket_record, admin_request, request_source="ui")

        except Exception as e:
            logger.error(f"Error processing admin request: {str(e)}")
            return {
                "summary_intent": "error",
                "email_response": f"Error processing request for ticket #{ticket_id}: {str(e)}."
            }

    async def are_all_actions_completed(self, ticket: dict) -> bool:
        """Check if all GitHub actions are completed."""
        github_actions = ticket.get("details", {}).get("github", [])
        pending_actions = ticket.get("pending_actions", False)
        return all(action["status"] in ["completed", "revoked", "failed"] for action in github_actions) and not pending_actions

    async def process_email(self, email: dict, broadcast, existing_ticket: dict = None, email_content: str = None) -> dict:
        """Process an email through the workflow."""
        try:
            email_id = email["id"]
            subject = email["subject"]
            body = email["body"]
            sender = email["from"]
            thread_id = email.get("threadId", email_id)
            attachments = email.get("attachments", [])
            is_follow_up = bool(existing_ticket)

            existing_email = self.tickets_collection.find_one({
                "$or": [
                    {"email_id": email_id},
                    {"thread_id": thread_id, "email_chain.email_id": email_id}
                ]
            })
            if existing_email:
                logger.info(f"Email ID={email_id} or thread_id={thread_id} already processed")
                return {
                    "status": "success",
                    "ticket_id": existing_email["ado_ticket_id"],
                    "intent": existing_email.get("type_of_request", "unknown"),
                    "message": "Email already processed",
                    "actions": [],
                    "pending_actions": existing_email.get("pending_actions", False)
                }

            await broadcast({
                "type": "email_detected",
                "email_id": email_id,
                "subject": subject,
                "sender": sender
            })

            intent_result = await self.analyze_intent(subject, body, attachments)
            intent = intent_result["intent"]
            ticket_description = intent_result["ticket_description"]
            actions = intent_result["actions"]
            pending_actions = intent_result["pending_actions"] or (existing_ticket.get("pending_actions", False) if is_follow_up else False)
            repo_name = intent_result.get("repo_name", "unspecified")
            access_type = intent_result.get("access_type", "unspecified")
            github_username = intent_result.get("github_username", "unspecified")

            await broadcast({
                "type": "intent_analyzed",
                "email_id": email_id,
                "intent": intent,
                "pending_actions": pending_actions
            })

            if intent == "non_intent":
                if is_follow_up:
                    self.tickets_collection.update_one(
                        {"thread_id": thread_id},
                        {"$push": {
                            "email_chain": {
                                "email_id": email_id,
                                "from": sender,
                                "subject": subject,
                                "body": body,
                                "timestamp": email.get("received", datetime.now().isoformat()),
                                "attachments": [{"filename": a["filename"], "mimeType": a["mimeType"]} for a in attachments]
                            }
                        }}
                    )
                return {
                    "status": "success",
                    "intent": "non_intent",
                    "ticket_id": existing_ticket["ado_ticket_id"] if is_follow_up else None,
                    "message": "Non-intent email processed",
                    "actions": [],
                    "pending_actions": False
                }

            if intent == "request_summary" and is_follow_up:
                summary_result = await self.generate_summary_response(existing_ticket, f"Subject: {subject}\nBody: {body}")
                email_response = summary_result["email_response"]

                reply_result = await self.kernel.invoke(
                    self.kernel.plugins["email_sender"]["send_reply"],
                    to=sender,
                    subject=subject,
                    body=email_response,
                    thread_id=thread_id,
                    message_id=email_id,
                    attachments=attachments,
                    remediation=""
                )
                reply = reply_result.value if reply_result else None

                if reply:
                    self.tickets_collection.update_one(
                        {"thread_id": thread_id},
                        {"$push": {
                            "email_chain": {
                                "email_id": reply.get("message_id", str(uuid.uuid4())),
                                "from": os.getenv('EMAIL_ADDRESS', 'IT Support <support@quadranttechnologies.com>'),
                                "subject": subject,
                                "body": email_response,
                                "timestamp": datetime.now().isoformat(),
                                "attachments": []
                            }
                        }}
                    )
                    await broadcast({
                        "type": "email_reply",
                        "email_id": email_id,
                        "thread_id": thread_id,
                        "ticket_id": existing_ticket["ado_ticket_id"],
                        "timestamp": datetime.now().isoformat()
                    })

                return {
                    "status": "success",
                    "ticket_id": existing_ticket["ado_ticket_id"],
                    "intent": intent,
                    "summary_intent": summary_result["summary_intent"],
                    "actions": [],
                    "pending_actions": False
                }

            ticket_id = existing_ticket["ado_ticket_id"] if is_follow_up else None
            github_result = None
            completed_actions = []

            if is_follow_up and intent in ["github_access_request", "github_revoke_access"]:
                ticket_id = existing_ticket["ado_ticket_id"]
                github_details = {
                    "request_type": intent,
                    "repo_name": repo_name,
                    "username": github_username,
                    "access_type": access_type if intent == "github_access_request" else "unspecified",
                    "status": "pending",
                    "message": f"Processing {intent} for {github_username} on {repo_name}"
                }

                update_operation = {
                    "$push": {
                        "details.github": github_details,
                        "updates": {
                            "status": "Doing",
                            "comment": github_details["message"],
                            "revision_id": f"git-{intent.split('_')[1]}-{ticket_id}-{len(existing_ticket.get('updates', [])) + 1}",
                            "email_sent": False,
                            "email_message_id": None,
                            "email_timestamp": datetime.now().isoformat()
                        }
                    },
                    "$set": {
                        "pending_actions": pending_actions
                    }
                }
                self.tickets_collection.update_one({"ado_ticket_id": ticket_id}, update_operation)

                if intent == "github_access_request":
                    github_result = await self.kernel.invoke(
                        self.kernel.plugins["git"]["grant_repo_access"],
                        repo_name=repo_name,
                        github_username=github_username,
                        access_type=access_type
                    )
                    github_result = github_result.value if github_result else {"success": False, "message": "GitHub grant action failed"}
                    github_details["status"] = "completed" if github_result["success"] else "failed"
                    github_details["message"] = github_result["message"]
                    completed_actions.append({"action": "grant_access", "completed": github_result["success"]})
                else:
                    github_result = await self.kernel.invoke(
                        self.kernel.plugins["git"]["revoke_repo_access"],
                        repo_name=repo_name,
                        github_username=github_username
                    )
                    github_result = github_result.value if github_result else {"success": False, "message": "GitHub revoke action failed"}
                    github_details["status"] = "revoked" if github_result["success"] else "failed"
                    github_details["message"] = github_result["message"]
                    completed_actions.append({"action": "revoke_access", "completed": github_result["success"]})
                    pending_actions = False

                self.tickets_collection.update_one(
                    {"ado_ticket_id": ticket_id, "details.github": {"$elemMatch": {"repo_name": repo_name, "username": github_username, "request_type": intent}}},
                    {
                        "$set": {
                            "details.github.$[elem].status": github_details["status"],
                            "details.github.$[elem].message": github_details["message"],
                            "pending_actions": pending_actions
                        },
                        "$push": {
                            "updates": {
                                "status": github_details["status"],
                                "comment": github_details["message"],
                                "revision_id": f"git-result-{ticket_id}-{len(existing_ticket.get('updates', [])) + 2}",
                                "email_sent": False,
                                "email_message_id": None,
                                "email_timestamp": datetime.now().isoformat()
                            }
                        }
                    },
                    array_filters=[{"elem.repo_name": repo_name, "elem.username": github_username, "elem.request_type": intent}]
                )

                updated_ticket = self.tickets_collection.find_one({"ado_ticket_id": ticket_id})
                await self.send_to_milvus(updated_ticket)

                await broadcast({
                    "type": "github_action",
                    "email_id": email_id,
                    "ticket_id": ticket_id,
                    "success": github_result["success"],
                    "message": github_details["message"]
                })

                all_completed = await self.are_all_actions_completed(updated_ticket)
                ado_status = "Done" if all_completed else "Doing"
                await self.kernel.invoke(
                    self.kernel.plugins["ado"]["update_ticket"],
                    ticket_id=ticket_id,
                    status=ado_status,
                    comment=github_details["message"]
                )

                updates_result = await self.kernel.invoke(
                    self.kernel.plugins["ado"]["get_ticket_updates"],
                    ticket_id=ticket_id
                )
                updates = updates_result.value if updates_result else []
                update_result = await self.analyze_ticket_update(ticket_id, updates, attachments)
                email_response = update_result["email_response"]
                remediation = update_result["remediation"]

                reply_result = await self.kernel.invoke(
                    self.kernel.plugins["email_sender"]["send_reply"],
                    to=sender,
                    subject=subject,
                    body=email_response,
                    thread_id=thread_id,
                    message_id=email_id,
                    attachments=attachments,
                    remediation=remediation
                )
                reply = reply_result.value if reply_result else None

                if reply:
                    self.tickets_collection.update_one(
                        {"thread_id": thread_id},
                        {"$push": {
                            "email_chain": {
                                "email_id": reply.get("message_id", str(uuid.uuid4())),
                                "from": os.getenv('EMAIL_ADDRESS', 'IT Support <support@quadranttechnologies.com>'),
                                "subject": subject,
                                "body": email_response,
                                "timestamp": datetime.now().isoformat(),
                                "attachments": []
                            }
                        }}
                    )
                    await broadcast({
                        "type": "email_reply",
                        "email_id": email_id,
                        "thread_id": thread_id,
                        "ticket_id": ticket_id,
                        "timestamp": datetime.now().isoformat()
                    })

                return {
                    "status": "success",
                    "ticket_id": ticket_id,
                    "intent": intent,
                    "github": github_result,
                    "actions": completed_actions,
                    "pending_actions": pending_actions
                }

            ticket_result = await self.kernel.invoke(
                self.kernel.plugins["ado"]["create_ticket"],
                title=subject,
                description=ticket_description,
                email_content=email_content,
                attachments=attachments
            )
            if not ticket_result or not ticket_result.value:
                logger.error(f"Failed to create ticket for email ID={email_id}")
                return {"status": "error", "message": "Ticket creation failed"}

            ticket_data = ticket_result.value
            ticket_id = ticket_data["id"]
            ado_url = ticket_data["url"]

            await broadcast({
                "type": "ticket_created",
                "email_id": email_id,
                "ticket_id": ticket_id,
                "ado_url": ado_url,
                "intent": intent
            })

            # Search Milvus for similar tickets
            has_matches, matching_ticket = await self.search_milvus_for_solution(subject, ticket_description)
            remediation = ""
            ticket_record = {
                "ado_ticket_id": ticket_id,
                "sender": sender,
                "subject": subject,
                "thread_id": thread_id,
                "email_id": email_id,
                "ticket_title": subject,
                "ticket_description": ticket_description,
                "email_timestamp": datetime.now().isoformat(),
                "updates": [],
                "email_chain": [{
                    "email_id": email_id,
                    "from": sender,
                    "subject": subject,
                    "body": body,
                    "timestamp": email.get("received", datetime.now().isoformat()),
                    "attachments": [{"filename": a["filename"], "mimeType": a["mimeType"]} for a in attachments]
                }],
                "pending_actions": pending_actions,
                "type_of_request": intent,
                "details": {
                    "attachments": [{"filename": a["filename"], "mimeType": a["mimeType"]} for a in attachments]
                },
                "in_milvus": has_matches
            }

            if has_matches:
                remediation = await self.generate_remediation_from_milvus(matching_ticket)
                # Store ticket in MongoDB with match flag
                try:
                    self.tickets_collection.update_one(
                        {"ado_ticket_id": ticket_id},
                        {"$setOnInsert": ticket_record},
                        upsert=True
                    )
                except DuplicateKeyError:
                    logger.info(f"Duplicate ticket ID={ticket_id} found, updating existing ticket")
                    existing = self.tickets_collection.find_one({"ado_ticket_id": ticket_id})
                    if existing:
                        update_operation = {
                            "$set": {
                                "sender": sender,
                                "subject": subject,
                                "thread_id": thread_id,
                                "email_id": email_id,
                                "ticket_title": subject,
                                "ticket_description": ticket_description,
                                "email_timestamp": datetime.now().isoformat(),
                                "pending_actions": pending_actions,
                                "type_of_request": intent,
                                "in_milvus": has_matches
                            },
                            "$push": {
                                "email_chain": ticket_record["email_chain"][0]
                            }
                        }
                        self.tickets_collection.update_one(
                            {"ado_ticket_id": ticket_id},
                            update_operation
                        )
            else:
                # Store ticket in MongoDB
                try:
                    self.tickets_collection.update_one(
                        {"ado_ticket_id": ticket_id},
                        {"$setOnInsert": ticket_record},
                        upsert=True
                    )
                except DuplicateKeyError:
                    logger.info(f"Duplicate ticket ID={ticket_id} found, updating existing ticket")
                    existing = self.tickets_collection.find_one({"ado_ticket_id": ticket_id})
                    if existing:
                        update_operation = {
                            "$set": {
                                "sender": sender,
                                "subject": subject,
                                "thread_id": thread_id,
                                "email_id": email_id,
                                "ticket_title": subject,
                                "ticket_description": ticket_description,
                                "email_timestamp": datetime.now().isoformat(),
                                "pending_actions": pending_actions,
                                "type_of_request": intent,
                                "in_milvus": has_matches
                            },
                            "$push": {
                                "email_chain": ticket_record["email_chain"][0]
                            }
                        }
                        self.tickets_collection.update_one(
                            {"ado_ticket_id": ticket_id},
                            update_operation
                        )
                # Store ticket in Milvus
                await self.send_to_milvus(ticket_record)

            github_result = None
            completed_actions = []

            if intent == "github_access_request" and repo_name != "unspecified" and github_username != "unspecified":
                github_result = await self.kernel.invoke(
                    self.kernel.plugins["git"]["grant_repo_access"],
                    repo_name=repo_name,
                    github_username=github_username,
                    access_type=access_type
                )
                github_result = github_result.value if github_result else {"success": False, "message": "GitHub action failed"}
                github_details = {
                    "request_type": intent,
                    "repo_name": repo_name,
                    "username": github_username,
                    "access_type": access_type,
                    "status": "completed" if github_result["success"] else "failed",
                    "message": github_result["message"]
                }
                completed_actions.append({"action": "grant_access", "completed": github_result["success"]})

                await self.kernel.invoke(
                    self.kernel.plugins["ado"]["update_ticket"],
                    ticket_id=ticket_id,
                    status="Doing" if pending_actions else "Done",
                    comment=github_result["message"]
                )

                await broadcast({
                    "type": "github_action",
                    "email_id": email_id,
                    "ticket_id": ticket_id,
                    "success": github_result["success"],
                    "message": github_result["message"]
                })

                # Update ticket_record with GitHub details
                ticket_record["details"]["github"] = [github_details]
                # Update MongoDB with GitHub details
                self.tickets_collection.update_one(
                    {"ado_ticket_id": ticket_id},
                    {"$set": {"details.github": [github_details]}}
                )

            elif intent == "general_it_request":
                sender_username = sender.split('@')[0] if '@' in sender else sender
                detailed_description = f"User {sender_username}: {ticket_description}"
                ticket_record["details"]["general"] = [{
                    "request_type": "general_it_request",
                    "status": "pending",
                    "message": detailed_description,
                    "requester": sender_username
                }]
                ticket_record["ticket_description"] = detailed_description
                # Update MongoDB with general IT details
                self.tickets_collection.update_one(
                    {"ado_ticket_id": ticket_id},
                    {"$set": {
                        "ticket_description": detailed_description,
                        "details.general": ticket_record["details"]["general"]
                    }}
                )

            updates_result = await self.kernel.invoke(
                self.kernel.plugins["ado"]["get_ticket_updates"],
                ticket_id=ticket_id
            )
            updates = updates_result.value if updates_result else []
            update_result = await self.analyze_ticket_update(ticket_id, updates, attachments)
            email_response = update_result["email_response"]
            remediation = remediation or update_result["remediation"]

            reply_result = await self.kernel.invoke(
                self.kernel.plugins["email_sender"]["send_reply"],
                to=sender,
                subject=subject,
                body=email_response,
                thread_id=thread_id,
                message_id=email_id,
                attachments=attachments,
                remediation=remediation
            )
            reply = reply_result.value if reply_result else None

            if reply:
                self.tickets_collection.update_one(
                    {"thread_id": thread_id},
                    {"$push": {
                        "email_chain": {
                            "email_id": reply.get("message_id", str(uuid.uuid4())),
                            "from": os.getenv('EMAIL_ADDRESS', 'IT Support <support@quadranttechnologies.com>'),
                            "subject": subject,
                            "body": email_response,
                            "timestamp": datetime.now().isoformat(),
                            "attachments": []
                        }
                    }}
                )
                await broadcast({
                    "type": "email_reply",
                    "email_id": email_id,
                    "thread_id": thread_id,
                    "ticket_id": ticket_id,
                    "timestamp": datetime.now().isoformat()
                })

            return {
                "status": "success",
                "ticket_id": ticket_id,
                "intent": intent,
                "github": github_result,
                "actions": completed_actions,
                "pending_actions": pending_actions
            }

        except Exception as e:
            logger.error(f"Error processing email ID={email_id}: {str(e)}")
            await broadcast({
                "type": "error",
                "email_id": email_id,
                "message": f"Failed to process email: {str(e)}"
            })
            return {
                "status": "error",
                "message": f"Error processing email: {str(e)}",
                "ticket_id": None,
                "intent": intent,
                "actions": [],
                "pending_actions": False
            }