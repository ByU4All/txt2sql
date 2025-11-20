# check_milvus_status.py
from pymilvus import MilvusClient, utility, connections
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_milvus_collections():
    """Check the status of Milvus collections"""

    try:
        # Connect to Milvus
        connections.connect(host="localhost", port="19530")
        client = MilvusClient(uri="http://localhost:19530")

        # List all collections
        collections = utility.list_collections()
        logger.info(f"Available collections: {collections}")

        for collection_name in ["tables", "columns", "cells"]:
            if collection_name in collections:
                # Get load state
                load_state = client.get_load_state(collection_name=collection_name)
                logger.info(f"{collection_name}: {load_state}")

                # Get collection info
                from pymilvus import Collection
                collection = Collection(collection_name)
                logger.info(f"{collection_name} entities: {collection.num_entities}")
            else:
                logger.info(f"{collection_name}: Not found")

    except Exception as e:
        logger.error(f"Error checking collections: {e}")
    finally:
        connections.disconnect("default")


if __name__ == "__main__":
    check_milvus_collections()
