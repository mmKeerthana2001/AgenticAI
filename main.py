from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
import logging
import asyncio
from datetime import datetime
import uuid
import json
import os
from dotenv import load_dotenv
from semantic_kernel import Kernel
from email_reader import EmailReaderPlugin
from email_sender import EmailSenderPlugin
from ado import ADOPlugin
from git import GitPlugin
from sk_agent import SKAgent
from pydantic import BaseModel

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("agent.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load environment Prophecy
load_dotenv()

# Initialize MongoDB
mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))
db = mongo_client["email_agent"]
tickets_collection = db["tickets"]
tickets_collection.create_index("ado_ticket_id", unique=True)
tickets_collection.create_index("thread_id")
logger.info("Initialized MongoDB: email_agent.tickets")

# Global state
is_running = False
email_task = None
ticket_task = None
session_id = None
ticket_info = {}
websocket_clients = []
email_processing_lock = asyncio.Lock()
processed_emails = set()

def cleanup_temp_files(temp_files):
    """Delete temporary files."""
    for file_path in temp_files:
        try:
            if os.path.exists(file_path):
                os.unlink(file_path)
                logger.info(f"Deleted temporary file: {file_path}")
        except Exception as e:
            logger.error(f"Error deleting temporary file {file_path}: {str(e)}")

async def process_emails():
    """Poll for new emails and process them."""
    kernel = Kernel()
    kernel.add_plugin(EmailReaderPlugin(), plugin_name="email_reader")
    kernel.add_plugin(EmailSenderPlugin(), plugin_name="email_sender")
    kernel.add_plugin(ADOPlugin(), plugin_name="ado")
    kernel.add_plugin(GitPlugin(), plugin_name="git")
    agent = SKAgent(kernel, tickets_collection)
    logger.info(f"Registered plugins: {list(kernel.plugins.keys())}")

    valid_domain = "@quadranttechnologies.com"
    
    while is_running:
        try:
            logger.info("Checking for new unread emails...")
            email_result = await kernel.invoke(
                kernel.plugins["email_reader"]["fetch_new_emails"],
                limit=1
            )
            emails = email_result.value if email_result else []
            
            if not emails:
                logger.info("No new unread emails found.")
            else:
                for email in emails:
                    email_id = email["id"]
                    thread_id = email.get("threadId", email_id)
                    
                    async with email_processing_lock:
                        if email_id in processed_emails:
                            logger.info(f"Skipping already processed email ID={email_id}")
                            continue

                        existing_ticket = tickets_collection.find_one({
                            "$or": [
                                {"email_id": email_id},
                                {"thread_id": thread_id, "email_chain.email_id": email_id}
                            ]
                        })
                        if existing_ticket:
                            logger.info(f"Email ID={email_id} or thread_id={thread_id} already processed")
                            continue

                        processed_emails.add(email_id)

                    attachments = email.get("attachments", [])
                    temp_files = [a['path'] for a in attachments]
                    
                    sender_email = email.get("from", "")
                    is_valid_domain = valid_domain in sender_email
                    
                    await broadcast({
                        "type": "email_detected",
                        "subject": email['subject'],
                        "sender": sender_email,
                        "email_id": email_id,
                        "is_valid_domain": is_valid_domain
                    })
                    
                    if not is_valid_domain:
                        logger.warning(f"Unauthorized email from {sender_email}")
                        await broadcast({
                            "type": "spam_alert",
                            "email_id": email_id,
                            "subject": email['subject'],
                            "sender": sender_email,
                            "message": f"Email rejected: Sender not from authorized domain"
                        })
                        cleanup_temp_files(temp_files)
                        continue
                    
                    logger.info(f"Processing email - Subject: {email['subject']}, From: {sender_email}")

                    existing_ticket = tickets_collection.find_one({"thread_id": thread_id})

                    if existing_ticket and email_id in [e["email_id"] for e in existing_ticket.get("email_chain", [])]:
                        logger.info(f"Email ID={email_id} already in email_chain, skipping.")
                        cleanup_temp_files(temp_files)
                        continue

                    email_content = f"""From: {sender_email}
Subject: {email['subject']}
Date: {email.get('received', datetime.now().isoformat())}
To: {os.getenv('EMAIL_ADDRESS', 'Unknown')}

{email['body']}
"""

                    email_intent_result = await agent.analyze_intent(email["subject"], email["body"], attachments)
                    intent = email_intent_result.get("intent", "general_it_request")
                    logger.info(f"Email intent analysis: {email_intent_result}")

                    email_chain_entry = {
                        "email_id": email_id,
                        "from": sender_email,
                        "subject": email["subject"],
                        "body": email["body"],
                        "timestamp": email.get("received", datetime.now().isoformat()),
                        "attachments": [
                            {"filename": a["filename"], "mimeType": a["mimeType"]} 
                            for a in attachments
                        ]
                    }

                    if intent == "non_intent":
                        logger.info(f"Non-intent email detected (ID={email_id}). Adding to email_chain and stopping processing.")
                        if existing_ticket:
                            tickets_collection.update_one(
                                {"thread_id": thread_id},
                                {"$push": {"email_chain": email_chain_entry}}
                            )
                        else:
                            tickets_collection.update_one(
                                {"thread_id": thread_id},
                                {
                                    "$setOnInsert": {
                                        "ado_ticket_id": None,
                                        "sender": sender_email,
                                        "subject": email["subject"],
                                        "thread_id": thread_id,
                                        "email_id": email_id,
                                        "ticket_title": "Non-intent email",
                                        "ticket_description": email_intent_result["ticket_description"],
                                        "email_timestamp": datetime.now().isoformat(),
                                        "updates": [],
                                        "email_chain": [email_chain_entry],
                                        "pending_actions": False,
                                        "type_of_request": "non_intent",
                                        "details": {"attachments": [{"filename": a["filename"], "mimeType": a["mimeType"]} for a in attachments]},
                                        "in_milvus": False
                                    }
                                },
                                upsert=True
                            )
                        cleanup_temp_files(temp_files)
                        continue

                    if intent == "request_summary":
                        if not existing_ticket:
                            logger.warning(f"No existing ticket found for thread_id={thread_id} for summary request")
                            await broadcast({
                                "type": "error",
                                "email_id": email_id,
                                "message": "No existing ticket found for summary request"
                            })
                            email_response = (
                                f"Hi,\n\nI couldn't find an existing request associated with this thread. "
                                f"Please provide the ticket ID or more details, and I'll be happy to assist!\n\nBest,\nAgent\nIT Support"
                            )
                            reply_result = await kernel.invoke(
                                kernel.plugins["email_sender"]["send_reply"],
                                to=sender_email,
                                subject=email["subject"],
                                body=email_response,
                                thread_id=thread_id,
                                message_id=email_id,
                                attachments=attachments,
                                remediation=""
                            )
                            email_status = bool(reply_result and reply_result.value)
                            email_message_id = reply_result.value.get("message_id") if reply_result and reply_result.value else str(uuid.uuid4())

                            if email_status:
                                reply_chain_entry = {
                                    "email_id": email_message_id,
                                    "from": os.getenv('EMAIL_ADDRESS', 'IT Support <support@quadranttechnologies.com>'),
                                    "subject": email["subject"],
                                    "body": email_response,
                                    "timestamp": datetime.now().isoformat(),
                                    "attachments": []
                                }
                                tickets_collection.update_one(
                                    {"thread_id": thread_id},
                                    {
                                        "$setOnInsert": {
                                            "ado_ticket_id": None,
                                            "sender": sender_email,
                                            "subject": email["subject"],
                                            "thread_id": thread_id,
                                            "email_id": email_id,
                                            "ticket_title": "Summary Request",
                                            "ticket_description": "User requested summary but no ticket found",
                                            "email_timestamp": datetime.now().isoformat(),
                                            "updates": [],
                                            "pending_actions": False,
                                            "type_of_request": "request_summary",
                                            "details": {}
                                        },
                                        "$push": {
                                            "email_chain": {
                                                "$each": [email_chain_entry, reply_chain_entry]
                                            }
                                        }
                                    },
                                    upsert=True
                                )
                            cleanup_temp_files(temp_files)
                            continue

                        summary_result = await agent.generate_summary_response(existing_ticket, email_content)
                        email_response = summary_result["email_response"]

                        reply_result = await kernel.invoke(
                            kernel.plugins["email_sender"]["send_reply"],
                            to=sender_email,
                            subject=email["subject"],
                            body=email_response,
                            thread_id=thread_id,
                            message_id=email_id,
                            attachments=attachments,
                            remediation=""
                        )
                        email_status = bool(reply_result and reply_result.value)
                        email_message_id = reply_result.value.get("message_id") if reply_result and reply_result.value else str(uuid.uuid4())

                        if email_status:
                            reply_chain_entry = {
                                "email_id": email_message_id,
                                "from": os.getenv('EMAIL_ADDRESS', 'IT Support <support@quadranttechnologies.com>'),
                                "subject": email["subject"],
                                "body": email_response,
                                "timestamp": datetime.now().isoformat(),
                                "attachments": []
                            }
                            tickets_collection.update_one(
                                {"thread_id": thread_id},
                                {
                                    "$push": {
                                        "email_chain": {
                                            "$each": [email_chain_entry, reply_chain_entry]
                                        }
                                    }
                                }
                            )
                            await broadcast({
                                "type": "email_reply",
                                "email_id": email_id,
                                "ticket_id": existing_ticket["ado_ticket_id"],
                                "thread_id": thread_id,
                                "timestamp": datetime.now().isoformat()
                            })
                        cleanup_temp_files(temp_files)
                        continue

                    result = await agent.process_email(email, broadcast, existing_ticket, email_content)
                    logger.info(f"Agent result for email ID={email_id}: {result}")
                    
                    cleanup_temp_files(temp_files)
                    
                    if result["status"] == "success":
                        ticket_id = result["ticket_id"]
                        intent = result.get("intent", "general_it_request")
                        pending_actions = result.get("pending_actions", False)
                        is_follow_up = bool(existing_ticket)

                        repo_name = email_intent_result.get("repo_name")
                        github_username = email_intent_result.get("github_username")
                        access_type = email_intent_result.get("access_type", "read")
                        ticket_title = email_intent_result.get("ticket_title", email["subject"])
                        ticket_description = email_intent_result.get("ticket_description", f"IT request for {email['subject']}")

                        if not is_follow_up:
                            ticket_record = {
                                "ado_ticket_id": ticket_id,
                                "sender": sender_email,
                                "subject": email["subject"],
                                "thread_id": thread_id,
                                "email_id": email_id,
                                "ticket_title": ticket_title,
                                "ticket_description": ticket_description,
                                "email_timestamp": datetime.now().isoformat(),
                                "updates": [],
                                "email_chain": [email_chain_entry],
                                "pending_actions": pending_actions,
                                "type_of_request": "github" if intent.startswith("github_") else intent,
                                "details": {"attachments": [{"filename": a["filename"], "mimeType": a["mimeType"]} for a in attachments]},
                                "in_milvus": False
                            }
                            
                            if intent == "github_access_request":
                                github_details = {
                                    "request_type": intent,
                                    "repo_name": repo_name if repo_name and repo_name != "unspecified" else "",
                                    "username": github_username if github_username and github_username != "unspecified" else "",
                                    "access_type": access_type if access_type and access_type != "unspecified" else "read",
                                    "status": "completed" if result.get("github", {}).get("success") else "failed",
                                    "message": result.get("github", {}).get("message", "GitHub access request initiated")
                                }
                                ticket_record["details"]["github"] = [github_details]
                            elif intent == "general_it_request":
                                sender_username = sender_email.split('@')[0] if '@' in sender_email else sender_email
                                detailed_description = ticket_description
                                if sender_username.lower() not in detailed_description.lower():
                                    detailed_description = f"User {sender_username}: {detailed_description}"
                                general_details = {
                                    "request_type": "general_it_request",
                                    "status": "pending",
                                    "message": detailed_description,
                                    "requester": sender_username
                                }
                                ticket_record["details"]["general"] = [general_details]
                                ticket_record["ticket_description"] = detailed_description
                            
                            # Use update_one with $setOnInsert to prevent duplicates
                            tickets_collection.update_one(
                                {"ado_ticket_id": ticket_id},
                                {"$setOnInsert": ticket_record},
                                upsert=True
                            )
                            ado_url = f"https://dev.azure.com/{os.getenv('ADO_ORGANIZATION')}/{os.getenv('ADO_PROJECT')}/_workitems/edit/{ticket_id}"
                            await broadcast({
                                "type": "ticket_created",
                                "email_id": email_id,
                                "ticket_id": ticket_id,
                                "subject": email["subject"],
                                "intent": intent,
                                "request_type": intent,
                                "ado_url": ado_url
                            })
                        else:
                            if intent.startswith("github_"):
                                updated_ticket = tickets_collection.find_one({"ado_ticket_id": ticket_id})
                                all_completed = await agent.are_all_actions_completed(updated_ticket)
                                ado_status = "Done" if all_completed else "Doing"
                                
                                update_operation = {
                                    "$push": {
                                        "email_chain": email_chain_entry,
                                        "updates": {
                                            "status": ado_status,
                                            "comment": f"Processed {intent} for {github_username} on {repo_name}",
                                            "revision_id": f"git-{intent.split('_')[1]}-{ticket_id}-{len(updated_ticket.get('updates', [])) + 1}",
                                            "email_sent": False,
                                            "email_message_id": None,
                                            "email_timestamp": datetime.now().isoformat()
                                        }
                                    },
                                    "$set": {
                                        "pending_actions": pending_actions
                                    }
                                }
                                tickets_collection.update_one({"ado_ticket_id": ticket_id}, update_operation)
                                
                                await kernel.invoke(
                                    kernel.plugins["ado"]["update_ticket"],
                                    ticket_id=ticket_id,
                                    status=ado_status,
                                    comment=f"Processed {intent} for {github_username} on {repo_name}"
                                )
                                
                                await broadcast({
                                    "type": "ticket_updated",
                                    "email_id": email_id,
                                    "ticket_id": ticket_id,
                                    "status": ado_status,
                                    "request_type": intent,
                                    "comment": f"Processed {intent} for {github_username} on {repo_name}"
                                })
                            elif intent == "general_it_request":
                                status = "updated"
                                comment = f"Updated general IT request: {email['subject']}"
                                
                                update_operation = {
                                    "$push": {
                                        "updates": {
                                            "status": status,
                                            "comment": comment,
                                            "revision_id": f"general-update-{ticket_id}-{len(existing_ticket.get('updates', [])) + 1}",
                                            "email_sent": False,
                                            "email_message_id": None,
                                            "email_timestamp": datetime.now().isoformat()
                                        },
                                        "email_chain": email_chain_entry
                                    },
                                    "$set": {
                                        "pending_actions": False
                                    }
                                }
                                
                                general_details = {
                                    "request_type": "general_it_request",
                                    "status": status,
                                    "message": comment
                                }
                                
                                if "general" not in existing_ticket.get("details", {}):
                                    update_operation["$set"]["details.general"] = [general_details]
                                else:
                                    update_operation["$push"]["details.general"] = general_details
                                
                                tickets_collection.update_one({"ado_ticket_id": ticket_id}, update_operation)
                                
                                await broadcast({
                                    "type": "ticket_updated",
                                    "email_id": email_id,
                                    "ticket_id": ticket_id,
                                    "status": status,
                                    "request_type": intent,
                                    "comment": comment
                                })
                    else:
                        logger.error(f"Failed to process email ID={email_id}: {result['message']}")
                        await broadcast({
                            "type": "error",
                            "email_id": email_id,
                            "message": f"Failed to process email: {result['message']}"
                        })

            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Error in email processing loop: {str(e)}")
            await asyncio.sleep(10)

async def process_tickets():
    """Check for ADO ticket updates."""
    kernel = Kernel()
    kernel.add_plugin(ADOPlugin(), plugin_name="ado")
    kernel.add_plugin(EmailSenderPlugin(), plugin_name="email_sender")
    agent = SKAgent(kernel, tickets_collection)

    while is_running:
        try:
            logger.info(f"Checking for ADO ticket updates in session {session_id}...")
            tickets = tickets_collection.find()
            
            for ticket in tickets:
                ticket_id = ticket["ado_ticket_id"]
                if not ticket_id:
                    continue
                update_result = await kernel.invoke(
                    kernel.plugins["ado"]["get_ticket_updates"],
                    ticket_id=ticket_id
                )
                updates = update_result.value if update_result else []
                ticket_data = ticket_info.get(ticket_id, {"last_revision_id": 0})
                last_revision_id = ticket_data["last_revision_id"]
                
                new_updates = [u for u in updates if u['revision_id'] > last_revision_id]
                
                if new_updates:
                    logger.info(f"Found {len(new_updates)} new updates for ticket ID={ticket_id}")
                    attachments = ticket.get("details", {}).get("attachments", [])
                    update_result = await agent.analyze_ticket_update(ticket_id, new_updates, attachments)
                    
                    if update_result["update_intent"] != "error":
                        sender = ticket.get('sender', 'Unknown')
                        subject = ticket.get('subject', f"Update for Ticket {ticket_id}")
                        thread_id = ticket.get('thread_id', str(uuid.uuid4()))
                        email_id = str(uuid.uuid4())
                        
                        reply_result = await kernel.invoke(
                            kernel.plugins["email_sender"]["send_reply"],
                            to=sender,
                            subject=subject,
                            body=update_result["email_response"],
                            thread_id=thread_id,
                            message_id=email_id,
                            attachments=attachments,
                            remediation=update_result["remediation"]
                        )
                        email_status = bool(reply_result and reply_result.value)
                        email_message_id = reply_result.value.get("message_id") if reply_result and reply_result.value else email_id
                        
                        email_chain_entry = {
                            "email_id": email_message_id,
                            "from": os.getenv('EMAIL_ADDRESS', 'IT Support <support@quadranttechnologies.com>'),
                            "subject": subject,
                            "body": update_result["email_response"],
                            "timestamp": datetime.now().isoformat(),
                            "attachments": [
                                {"filename": a["filename"], "mimeType": a["mimeType"]} 
                                for a in attachments
                            ]
                        }
                        
                        existing_email_ids = [e["email_id"] for e in ticket.get("email_chain", [])]
                        update_operations = []
                        
                        for update in new_updates:
                            update_operation = {
                                "$push": {
                                    "updates": {
                                        "comment": update["comment"] or "No comment provided",
                                        "status": update["status"],
                                        "revision_id": update["revision_id"],
                                        "email_sent": email_status,
                                        "email_message_id": email_message_id,
                                        "email_timestamp": datetime.now().isoformat()
                                    }
                                }
                            }
                            update_operations.append(update_operation)
                        
                        if email_status and email_message_id not in existing_email_ids:
                            email_chain_operation = {
                                "$push": {
                                    "email_chain": email_chain_entry
                                }
                            }
                            update_operations.append(email_chain_operation)
                        
                        for operation in update_operations:
                            tickets_collection.update_one(
                                {"ado_ticket_id": ticket_id},
                                operation
                            )
                        
                        if email_status:
                            ticket_info[ticket_id] = {
                                "sender": sender,
                                "subject": subject,
                                "thread_id": thread_id,
                                "email_id": email_id,
                                "last_revision_id": max(u['revision_id'] for u in updates)
                            }
                            await broadcast({
                                "type": "email_reply",
                                "email_id": email_id,
                                "ticket_id": ticket_id,
                                "thread_id": thread_id,
                                "timestamp": datetime.now().isoformat()
                            })
                    else:
                        for update in new_updates:
                            tickets_collection.update_one(
                                {"ado_ticket_id": ticket_id},
                                {
                                    "$push": {
                                        "updates": {
                                            "comment": update["comment"] or "No comment provided",
                                            "status": update["status"],
                                            "revision_id": update["revision_id"],
                                            "email_sent": False,
                                            "email_message_id": None,
                                            "email_timestamp": datetime.now().isoformat()
                                        }
                                    }
                                }
                            )
                        ticket_info[ticket_id]["last_revision_id"] = max(u['revision_id'] for u in updates)
                    
                    # Sync updated ticket to Milvus
                    updated_ticket = tickets_collection.find_one({"ado_ticket_id": ticket_id})
                    await agent.send_to_milvus(updated_ticket)
            
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Error in ticket processing loop: {str(e)}")
            await asyncio.sleep(10)

@app.get("/run-agent")
async def run_agent():
    """Start the email and ticket tracking agent."""
    global is_running, email_task, ticket_task, session_id, ticket_info
    if is_running:
        logger.info("Agent is already running.")
        return {"status": "info", "message": "Agent is already running"}
    
    logger.info("Starting email and ticket tracking agent...")
    session_id = str(uuid.uuid4())
    ticket_info = {}
    kernel = Kernel()
    kernel.add_plugin(ADOPlugin(), plugin_name="ado")
    work_items_result = await kernel.invoke(
        kernel.plugins["ado"]["get_all_work_items"]
    )
    work_items = work_items_result.value if work_items_result else []
    for work_item in work_items:
        ticket_id = work_item['id']
        updates_result = await kernel.invoke(
            kernel.plugins["ado"]["get_ticket_updates"],
            ticket_id=ticket_id
        )
        updates = updates_result.value if updates_result else []
        last_revision_id = max((u['revision_id'] for u in updates), default=0)
        ticket_record = tickets_collection.find_one({"ado_ticket_id": ticket_id})
        ticket_info[ticket_id] = {
            "sender": ticket_record.get('sender', 'Unknown') if ticket_record else 'Unknown',
            "subject": ticket_record.get('subject', f"Update for Ticket {ticket_id}") if ticket_record else f"Update for Ticket {ticket_id}",
            "thread_id": ticket_record.get('thread_id', str(uuid.uuid4())) if ticket_record else str(uuid.uuid4()),
            "email_id": ticket_record.get('email_id', str(uuid.uuid4())) if ticket_record else str(uuid.uuid4()),
            "last_revision_id": last_revision_id
        }
    is_running = True
    email_task = asyncio.create_task(process_emails())
    ticket_task = asyncio.create_task(process_tickets())
    
    await broadcast({"type": "session", "session_id": session_id, "status": "started"})
    return {"status": "success", "message": f"Agent started with session ID={session_id}"}

class AdminRequest(BaseModel):
    ticket_id: int
    request: str

@app.post("/send-request")
async def send_request(admin_request: AdminRequest):
    """Handle admin request for ticket summary or update."""
    try:
        kernel = Kernel()
        kernel.add_plugin(EmailReaderPlugin(), plugin_name="email_reader")
        kernel.add_plugin(EmailSenderPlugin(), plugin_name="email_sender")
        kernel.add_plugin(ADOPlugin(), plugin_name="ado")
        kernel.add_plugin(GitPlugin(), plugin_name="git")
        agent = SKAgent(kernel, tickets_collection)

        result = await agent.process_admin_request(admin_request.ticket_id, admin_request.request)
        return {
            "status": "success",
            "summary_intent": result["summary_intent"],
            "response": result["email_response"]
        }
    except Exception as e:
        logger.error(f"Error in /send-request endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.get("/stop-agent")
async def stop_agent():
    """Stop the email and ticket tracking agent."""
    global is_running, email_task, ticket_task, session_id, ticket_info
    if not is_running:
        logger.info("Agent is not running.")
        return {"status": "info", "message": "Agent is not running"}
    
    logger.info(f"Stopping agent for session {session_id}...")
    is_running = False
    if email_task:
        email_task.cancel()
    if ticket_task:
        ticket_task.cancel()
    email_task = None
    ticket_task = None
    session_id = None
    ticket_info = {}
    processed_emails.clear()
    await broadcast({"type": "session", "session_id": None, "status": "stopped"})
    return {"status": "success", "message": "Agent stopped"}

@app.get("/tickets")
async def get_tickets():
    """Get all tickets from MongoDB."""
    try:
        tickets = list(tickets_collection.find({}, {"_id": 0}))
        logger.info(f"Returning {len(tickets)} tickets from /tickets endpoint")
        return {"status": "success", "tickets": tickets}
    except Exception as e:
        logger.error(f"Error fetching tickets: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/tickets/by-type/{request_type}")
async def get_tickets_by_type(request_type: str):
    """Get tickets filtered by request type."""
    try:
        tickets = list(tickets_collection.find({"type_of_request": request_type}, {"_id": 0}))
        logger.info(f"Returning {len(tickets)} tickets of type {request_type}")
        return {"status": "success", "tickets": tickets}
    except Exception as e:
        logger.error(f"Error fetching tickets by type: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/logs")
async def get_logs():
    """Get recent logs from agent.log."""
    try:
        with open("agent.log", "r") as f:
            logs = f.readlines()[-50:]
        return {"status": "success", "logs": logs}
    except Exception as e:
        logger.error(f"Error fetching logs: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.get("/status")
async def get_status():
    """Get the current status of the agent."""
    return {"status": "success", "is_running": is_running, "session_id": session_id}

@app.get("/request-types")
async def get_request_types():
    """Get all distinct request types in the system."""
    try:
        distinct_types = tickets_collection.distinct("type_of_request")
        return {"status": "success", "request_types": distinct_types}
    except Exception as e:
        logger.error(f"Error fetching request types: {str(e)}")
        return {"status": "error", "message": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle WebSocket connections."""
    await websocket.accept()
    logger.info("WebSocket connection accepted")
    websocket_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
        websocket_clients.remove(websocket)
    finally:
        logger.info("WebSocket connection closed")

async def broadcast(message):
    """Broadcast a message to all WebSocket connections."""
    logger.info(f"Broadcasting message: {message}")
    for client in websocket_clients:
        try:
            await client.send_json(message)
        except Exception as e:
            logger.error(f"Error broadcasting to WebSocket: {str(e)}")

@app.get("/")
async def root():
    return {"message": "Email Agent API is running"}