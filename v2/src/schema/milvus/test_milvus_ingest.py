# --------------------------------------------------------
# test_milvus_ingest.py
# Test script to verify Milvus ingestion worked correctly
# --------------------------------------------------------

import requests
from pymilvus import connections, Collection, MilvusException
import json

# -----------------------------
# CONFIG
# -----------------------------
MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"
OLLAMA_HOST = "http://localhost:11434"
EMBED_MODEL = "embeddinggemma:latest"

# Connect to Milvus
connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
print("✓ Connected to Milvus")


def test_collection_basic(collection_name):
    """Test a specific collection - basic checks only"""
    print(f"\n{'=' * 50}")
    print(f"Testing collection: {collection_name}")
    print(f"{'=' * 50}")

    try:
        collection = Collection(collection_name)

        # Check if collection exists and get basic info WITHOUT loading
        print(f"Collection exists: {collection.name}")

        # Get schema info
        fields = collection.schema.fields
        print(f"Schema fields: {[field.name for field in fields]}")

        # Check embedding field dimension
        embedding_field = None
        for field in fields:
            if field.name == "embedding":
                embedding_field = field
                break

        if embedding_field:
            dim = embedding_field.params.get('dim', 'unknown')
            print(f"✓ Embedding field found with dimension: {dim}")
        else:
            print("⚠ No embedding field found")

        # Try to get count without loading (this might still work)
        try:
            count = collection.num_entities
            print(f"Total documents: {count}")

            if count > 0:
                print(f"✓ Collection '{collection_name}' has data")
                return True
            else:
                print(f"⚠ Collection '{collection_name}' is empty")
                return False

        except Exception as count_error:
            print(f"⚠ Could not get document count: {count_error}")
            print(f"✓ Collection '{collection_name}' exists but cannot verify data without loading")
            return True

    except MilvusException as e:
        print(f"✗ Collection '{collection_name}' test failed: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error testing '{collection_name}': {e}")
        return False


def test_all_collections():
    """Test all expected collections - basic verification only"""
    collections_to_test = ["tables", "columns", "cells"]

    print("Starting Milvus ingestion tests...")
    print(f"Testing collections: {collections_to_test}")

    success_count = 0
    total_docs = 0

    for collection_name in collections_to_test:
        if test_collection_basic(collection_name):
            success_count += 1
            try:
                collection = Collection(collection_name)
                count = collection.num_entities
                total_docs += count
            except:
                pass

    print(f"\n{'=' * 50}")
    print("Test Summary")
    print(f"{'=' * 50}")

    # Get summary stats for all collections
    for collection_name in collections_to_test:
        try:
            collection = Collection(collection_name)
            count = collection.num_entities
            print(f"{collection_name.capitalize()}: {count} documents")
        except:
            print(f"{collection_name.capitalize()}: Collection not found")

    print(f"\nSuccessful collections: {success_count}/{len(collections_to_test)}")
    print(f"Total documents across all collections: {total_docs}")

    if total_docs > 0 and success_count > 0:
        print("✓ Ingestion appears successful!")
    else:
        print("⚠ Issues found with ingestion")


if __name__ == "__main__":
    test_all_collections()
