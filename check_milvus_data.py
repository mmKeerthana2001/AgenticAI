
from pymilvus import connections, Collection
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_milvus_data():
    try:
        # Connect to Milvus
        connections.connect(host="localhost", port="19530")
        logger.info("Connected to Milvus successfully")

        # Load the ticket_details collection
        collection = Collection("ticket_details")
        collection.load()
        logger.info(f"Collection 'ticket_details' loaded. Total entities: {collection.num_entities}")

        # Query all entities
        results = collection.query(
            expr="ado_ticket_id >= 0",  # Fetch all entities
            output_fields=["ado_ticket_id", "ticket_title", "ticket_description", "updates"],
            limit=10
        )

        if not results:
            logger.warning("No entities found in ticket_details collection")
            return

        # Print each entity's data
        for result in results:
            logger.info("Found entity:")
            logger.info(f"  ado_ticket_id: {result['ado_ticket_id']}")
            logger.info(f"  ticket_title: {result['ticket_title']}")
            logger.info(f"  ticket_description: {result['ticket_description']}")
            logger.info(f"  updates: {result['updates']}")

    except Exception as e:
        logger.error(f"Error querying Milvus: {str(e)}")

if __name__ == "__main__":
    check_milvus_data()
