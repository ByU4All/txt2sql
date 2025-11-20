# reset_and_load_collections.py
from pymilvus import MilvusClient, utility, connections, Collection
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def reset_and_load_collections():
    """Reset stuck collections and load them properly"""

    try:
        # Connect to Milvus
        connections.connect(host="localhost", port="19530")
        client = MilvusClient(uri="http://localhost:19530")

        for collection_name in ["tables", "columns", "cells"]:
            if utility.has_collection(collection_name):
                logger.info(f"Processing collection: {collection_name}")

                # Get current state
                load_state = client.get_load_state(collection_name=collection_name)
                logger.info(f"Current state: {load_state}")

                # If stuck in loading or we want to refresh, release first
                if load_state['state'].name in ['Loading', 'Loaded']:
                    logger.info(f"Releasing collection: {collection_name}")
                    client.release_collection(collection_name=collection_name)
                    time.sleep(2)  # Wait a bit

                # Now load the collection
                logger.info(f"Loading collection: {collection_name}")
                client.load_collection(collection_name=collection_name)

                # Wait for loading to complete
                max_wait = 60  # seconds
                start_time = time.time()

                while time.time() - start_time < max_wait:
                    load_state = client.get_load_state(collection_name=collection_name)

                    if load_state['state'].name == 'Loaded':
                        logger.info(f"✅ Collection {collection_name} loaded successfully!")
                        break
                    elif load_state['state'].name == 'Loading':
                        progress = load_state.get('progress', 'unknown')
                        logger.info(f"Loading {collection_name}: {progress}%")
                        time.sleep(3)
                    else:
                        logger.warning(f"Unexpected state for {collection_name}: {load_state}")
                        time.sleep(3)
                else:
                    logger.error(f"❌ Timeout loading {collection_name}")

        # Final status check
        logger.info("\n=== FINAL STATUS ===")
        for collection_name in ["tables", "columns", "cells"]:
            if utility.has_collection(collection_name):
                load_state = client.get_load_state(collection_name=collection_name)
                collection = Collection(collection_name)
                logger.info(f"{collection_name}: {load_state} - {collection.num_entities} entities")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        connections.disconnect("default")


if __name__ == "__main__":
    reset_and_load_collections()
