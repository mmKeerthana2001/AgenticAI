import logging
from pymilvus import MilvusClient, DataType, CollectionSchema, FieldSchema
from sentence_transformers import SentenceTransformer
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

class MilvusClientWrapper:
    def __init__(self, uri: str, collection_name: str = "it_tickets"):
        self.client = MilvusClient(uri=uri)
        self.collection_name = collection_name
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self._initialize_collection()
        logger.info(f"Initialized Milvus client with URI {uri} and collection {collection_name}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def _initialize_collection(self):
        try:
            # Check if collection exists
            collections = self.client.list_collections()
            if self.collection_name in collections:
                # Verify index existence
                indexes = self.client.list_indexes(self.collection_name)
                if not indexes or "embedding" not in [idx["field_name"] for idx in indexes]:
                    logger.warning(f"Collection {self.collection_name} exists but has no valid index. Dropping collection.")
                    self.client.drop_collection(self.collection_name)
                else:
                    logger.info(f"Collection {self.collection_name} exists with valid index.")
                    return

            # Create collection
            fields = [
                FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema(name="ado_ticket_id", dtype=DataType.VARCHAR, max_length=50),
                FieldSchema(name="ticket_title", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(name="text_type", dtype=DataType.VARCHAR, max_length=50),
                FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
                FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384)
            ]
            schema = CollectionSchema(fields=fields, description="IT tickets collection")
            self.client.create_collection(
                collection_name=self.collection_name,
                schema=schema
            )
            logger.info(f"Created Milvus collection {self.collection_name}")

            # Create index
            index_params = {
                "index_type": "HNSW",
                "metric_type": "L2",
                "params": {"M": 16, "efConstruction": 200}
            }
            self.client.create_index(
                collection_name=self.collection_name,
                field_name="embedding",
                index_params=index_params
            )
            logger.info(f"Created HNSW index on embedding field for {self.collection_name}")

            # Load collection after index creation
            self.client.load_collection(self.collection_name)
            logger.info(f"Loaded collection {self.collection_name}")
        except Exception as e:
            logger.error(f"Error initializing collection {self.collection_name}: {str(e)}")
            raise

    def store_ticket(self, ticket: dict):
        try:
            ado_ticket_id = str(ticket.get("ado_ticket_id", ""))
            ticket_title = ticket.get("ticket_title", "")
            ticket_description = ticket.get("ticket_description", "")
            updates = ticket.get("updates", [])
            update_comments = [u["comment"] for u in updates if u.get("comment")]
            texts = [
                {"text": ticket_title, "type": "title"},
                {"text": ticket_description, "type": "description"}
            ] + [{"text": comment, "type": "comment"} for comment in update_comments]
            data = []
            for item in texts:
                text = item["text"]
                text_type = item["type"]
                if not text.strip():
                    logger.warning(f"Empty {text_type} for ticket {ado_ticket_id}")
                    continue
                embedding = self.model.encode(text).tolist()
                data.append({
                    "ado_ticket_id": ado_ticket_id,
                    "ticket_title": ticket_title,
                    "text_type": text_type,
                    "text": text[:65535],
                    "embedding": embedding
                })
            if data:
                self.client.insert(self.collection_name, data)
                logger.info(f"Stored {len(data)} embeddings for ticket {ado_ticket_id} in Milvus")
            else:
                logger.warning(f"No valid texts to embed for ticket {ado_ticket_id}")
        except Exception as e:
            logger.error(f"Error storing ticket {ado_ticket_id} in Milvus: {str(e)}")

    def update_ticket(self, ticket: dict):
        try:
            ado_ticket_id = str(ticket.get("ado_ticket_id", ""))
            self.client.delete(
                collection_name=self.collection_name,
                filter=f"ado_ticket_id == '{ado_ticket_id}'"
            )
            self.store_ticket(ticket)
            logger.info(f"Updated ticket {ado_ticket_id} in Milvus")
        except Exception as e:
            logger.error(f"Error updating ticket {ado_ticket_id} in Milvus: {str(e)}")

    def search_similar_tickets(self, query_text: str, limit: int = 5, type_of_request: str = None) -> list:
        try:
            if not query_text.strip():
                logger.warning("Empty query text for similarity search")
                return []
            embedding = self.model.encode(query_text).tolist()
            self.client.load_collection(self.collection_name)
            filter_expr = "text_type in ['title', 'description']"
            if type_of_request:
                filter_expr += f" && type_of_request == \"{type_of_request}\""
            results = self.client.search(
                collection_name=self.collection_name,
                data=[embedding],
                limit=limit,
                output_fields=["ado_ticket_id", "ticket_title", "text_type", "text"],
                filter=filter_expr
            )
            tickets = []
            seen_tickets = set()
            for hit in results[0]:
                ado_ticket_id = hit["entity"]["ado_ticket_id"]
                if ado_ticket_id not in seen_tickets:
                    tickets.append({
                        "ado_ticket_id": ado_ticket_id,
                        "ticket_title": hit["entity"]["ticket_title"],
                        "ticket_description": hit["entity"]["text"],
                        "text_type": hit["entity"]["text_type"],
                        "distance": hit["distance"]
                    })
                    seen_tickets.add(ado_ticket_id)
            logger.info(f"Found {len(tickets)} similar tickets for query")
            return tickets[:limit]
        except Exception as e:
            logger.error(f"Error searching similar tickets: {str(e)}")
            return []

    def query_similar_tickets(self, query_text: str, limit: int = 5) -> list:
        return self.search_similar_tickets(query_text, limit)