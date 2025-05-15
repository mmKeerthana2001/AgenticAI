import logging
from pymilvus import connections, Collection, utility, FieldSchema, CollectionSchema, DataType

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clear_milvus_collection():
    try:
        # Connect to Milvus
        connections.connect(host="localhost", port="19530")
        logger.info("Connected to Milvus")

        # Drop and recreate collection
        if utility.has_collection("ticket_details"):
            utility.drop_collection("ticket_details")
            logger.info("Dropped ticket_details collection")

        schema = CollectionSchema([
            FieldSchema("ado_ticket_id", DataType.INT64, is_primary=True),
            FieldSchema("ticket_title", DataType.VARCHAR, max_length=512),
            FieldSchema("ticket_description", DataType.VARCHAR, max_length=4096),
            FieldSchema("updates", DataType.VARCHAR, max_length=8192),
            FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=384)
        ])
        collection = Collection("ticket_details", schema)
        collection.create_index("embedding", {"metric_type": "L2", "index_type": "IVF_FLAT", "params": {"nlist": 1024}})
        logger.info("Recreated ticket_details collection")

        # Verify collection is empty
        collection.load()
        if collection.num_entities == 0:
            logger.info("Collection is empty")
        else:
            logger.warning(f"Collection contains {collection.num_entities} records")

    except Exception as e:
        logger.error(f"Error: {str(e)}")
    finally:
        connections.disconnect(alias="default")
        logger.info("Disconnected from Milvus")

if __name__ == "__main__":
    clear_milvus_collection()