import numpy as np
import requests
from src.retrieval.retrievers.hybrid_retriever import HybridRetriever
# from ..retrievers.hybrid_retriever import HybridRetriever
import logging
import sys

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_ollama_embedding(text: str, model: str = "embeddinggemma:latest") -> list:
    """Get embedding from Ollama"""
    try:
        response = requests.post(
            "http://localhost:11434/api/embeddings",
            json={"model": model, "prompt": text}
        )
        if response.status_code == 200:
            return response.json()["embedding"]
        else:
            logger.error(f"Ollama request failed: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Failed to get embedding: {e}")
        return None


def test_hybrid_retriever(_query_text = "Show each customer along with their total number of sales."):
    """Test the HybridRetriever functionality"""

    retriever = None
    try:
        # Initialize the retriever
        logger.info("Initializing HybridRetriever...")
        retriever = HybridRetriever(
            milvus_host="localhost",
            milvus_port=19530,
            neo4j_uri="bolt://localhost:7687",
            neo4j_user="neo4j",
            neo4j_password="password"
        )

        # Initialize collections
        logger.info("Initializing Milvus collections...")
        collections_ready = retriever.initialize_collections()

        if not collections_ready:
            logger.error("❌ Cannot proceed without Milvus collections")
            logger.info("📋 Next steps:")
            logger.info("   1. Ensure PostgreSQL is running with sample data")
            logger.info("   2. Run: python src/schema/milvus_ingest.py")
            logger.info("   3. Wait for ingestion to complete")
            logger.info("   4. Run this test again")
            return False

        # Test with minimal retrieval if some collections are missing
        logger.info("🧪 Testing hybrid retrieval with available collections...")

        # Test query
        query_text = _query_text
        logger.info(f"\n=== Testing query: '{query_text}' ===")

        # Generate embedding
        logger.info("Generating query embedding...")
        query_embedding = get_ollama_embedding(query_text)

        if not query_embedding:
            logger.error("Failed to generate embedding - is Ollama running?")
            return False

        query_embedding = np.array(query_embedding, dtype=np.float32)
        logger.info(f"Generated embedding with dimension: {len(query_embedding)}")

        # Perform retrieval
        logger.info("Performing hybrid retrieval...")
        result = retriever.retrieve(
            query_text=query_text,
            query_embedding=query_embedding,
            top_k_tables=5,
            top_n_columns=10,
            top_m_values=5
        )

        # Display formatted result
        import json
        print("=== USER QUERY ===")
        print(_query_text)
        logger.info("=== RETRIEVAL RESULT ===")
        print("=== RETRIEVAL RESULT ===")
        print(json.dumps(result, indent=2))

        logger.info("\n=== SUCCESS ===")
        logger.info("✅ Hybrid retrieval completed successfully!")
        return True

    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if retriever:
            retriever.close()
            logger.info("Connections closed")


def check_prerequisites():
    """Check if required services are running"""
    logger.info("🔍 Checking prerequisites...")

    # Check Ollama
    try:
        response = requests.get("http://localhost:11434/api/version", timeout=5)
        if response.status_code == 200:
            logger.info("✅ Ollama is running")
        else:
            logger.warning("⚠️ Ollama may not be running properly")
    except:
        logger.error("❌ Ollama is not accessible at localhost:11434")
        return False

    # Check Milvus
    try:
        from pymilvus import connections
        connections.connect("test", host="localhost", port="19530")
        connections.disconnect("test")
        logger.info("✅ Milvus is accessible")
    except:
        logger.error("❌ Milvus is not accessible at localhost:19530")
        return False

    # Check Neo4j
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        with driver.session() as session:
            session.run("RETURN 1")
        driver.close()
        logger.info("✅ Neo4j is accessible")
    except:
        logger.error("❌ Neo4j is not accessible at localhost:7687")
        return False

    return True


if __name__ == "__main__":
    if not check_prerequisites():
        logger.error("❌ Prerequisites not met. Please start required services.")
        sys.exit(1)
    user_query = "How many interactions each employee had with each customer?"
    success = test_hybrid_retriever(_query_text = user_query)
    sys.exit(0 if success else 1)
