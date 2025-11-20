# --------------------------------------------------------
# milvus_ingest.py - Fixed version
# Ingest vector docs into Milvus with correct schema
# --------------------------------------------------------

import json
import requests
from pymilvus import (
    connections, FieldSchema, CollectionSchema,
    DataType, Collection, MilvusException, utility
)
import os

# -----------------------------
# CONFIG
# -----------------------------
MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"

TABLES_PATH = "/data/vector_docs/tables_embeddings.json"
COLUMNS_PATH = "/data/vector_docs/columns_embeddings.json"
CELLS_PATH = "/data/vector_docs/cells_candidates.json"

OLLAMA_HOST = "http://localhost:11434"
EMBED_MODEL = "embeddinggemma:latest"

# --------------------------------------------------------
# Connect to Milvus
# --------------------------------------------------------
connections.connect("default", host=MILVUS_HOST, port=MILVUS_PORT)
print("✓ Connected to Milvus")


# --------------------------------------------------------
# Ollama embedding functions
# --------------------------------------------------------
def get_embedding_dimension(model_name):
    """Get embedding dimension by making a test call"""
    test_embedding = get_ollama_embedding("test", model_name)
    return len(test_embedding) if test_embedding else 768


def get_ollama_embedding(text, model_name):
    """Get embedding from Ollama"""
    response = requests.post(
        f"{OLLAMA_HOST}/api/embeddings",
        json={"model": model_name, "prompt": text}
    )
    if response.status_code == 200:
        return response.json()["embedding"]
    else:
        raise Exception(f"Ollama embedding failed: {response.text}")


def get_ollama_embeddings_batch(texts, model_name):
    """Get embeddings for multiple texts"""
    embeddings = []
    for text in texts:
        embedding = get_ollama_embedding(text, model_name)
        embeddings.append(embedding)
    return embeddings


# --------------------------------------------------------
# Load embedding model and get dimension
# --------------------------------------------------------
D = get_embedding_dimension(EMBED_MODEL)
print(f"✓ Embedding dim = {D}")


# --------------------------------------------------------
# Collection schemas matching HybridRetriever expectations
# --------------------------------------------------------
def create_tables_collection():
    """Create tables collection with correct schema"""
    if utility.has_collection("tables"):
        utility.drop_collection("tables")
        print("→ Dropped existing tables collection")

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="table_name", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="schema_name", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="description", dtype=DataType.VARCHAR, max_length=1000),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=D)
    ]

    schema = CollectionSchema(fields, description="Tables collection")
    collection = Collection("tables", schema)

    collection.create_index(
        field_name="embedding",
        index_params={
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128}
        }
    )
    print("✓ Created tables collection")
    return collection


def create_columns_collection():
    """Create columns collection with correct schema"""
    if utility.has_collection("columns"):
        utility.drop_collection("columns")
        print("→ Dropped existing columns collection")

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="table_name", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="column_name", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="data_type", dtype=DataType.VARCHAR, max_length=100),
        FieldSchema(name="description", dtype=DataType.VARCHAR, max_length=1000),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=D)
    ]

    schema = CollectionSchema(fields, description="Columns collection")
    collection = Collection("columns", schema)

    collection.create_index(
        field_name="embedding",
        index_params={
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128}
        }
    )
    print("✓ Created columns collection")
    return collection


def create_cells_collection():
    """Create cells collection with correct schema"""
    if utility.has_collection("cells"):
        utility.drop_collection("cells")
        print("→ Dropped existing cells collection")

    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="table_name", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="column_name", dtype=DataType.VARCHAR, max_length=255),
        FieldSchema(name="cell_value", dtype=DataType.VARCHAR, max_length=1000),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=D)
    ]

    schema = CollectionSchema(fields, description="Cells collection")
    collection = Collection("cells", schema)

    collection.create_index(
        field_name="embedding",
        index_params={
            "metric_type": "COSINE",
            "index_type": "IVF_FLAT",
            "params": {"nlist": 128}
        }
    )
    print("✓ Created cells collection")
    return collection


# --------------------------------------------------------
# Ingest functions for each collection type
# --------------------------------------------------------
def ingest_tables(docs):
    """Ingest tables data"""
    collection = create_tables_collection()

    print(f"→ Ingesting {len(docs)} table documents")

    for chunk_start in range(0, len(docs), 64):
        batch = docs[chunk_start: chunk_start + 64]

        table_names = []
        schema_names = []
        descriptions = []
        texts = []

        for doc in batch:
            metadata = doc.get("metadata", {})
            table_names.append(metadata.get("table_name", "unknown"))
            schema_names.append(metadata.get("schema_name", "public"))
            descriptions.append(doc.get("text", ""))
            texts.append(doc.get("text", ""))

        embeddings = get_ollama_embeddings_batch(texts, EMBED_MODEL)

        collection.insert([
            table_names,  # table_name
            schema_names,  # schema_name
            descriptions,  # description
            embeddings  # embedding
        ])

    collection.flush()
    print("✓ Tables ingestion completed")


def ingest_columns(docs):
    """Ingest columns data"""
    collection = create_columns_collection()

    print(f"→ Ingesting {len(docs)} column documents")

    for chunk_start in range(0, len(docs), 64):
        batch = docs[chunk_start: chunk_start + 64]

        table_names = []
        column_names = []
        data_types = []
        descriptions = []
        texts = []

        for doc in batch:
            metadata = doc.get("metadata", {})
            table_names.append(metadata.get("table_name", "unknown"))
            column_names.append(metadata.get("column_name", "unknown"))
            data_types.append(metadata.get("data_type", "unknown"))
            descriptions.append(doc.get("text", ""))
            texts.append(doc.get("text", ""))

        embeddings = get_ollama_embeddings_batch(texts, EMBED_MODEL)

        collection.insert([
            table_names,  # table_name
            column_names,  # column_name
            data_types,  # data_type
            descriptions,  # description
            embeddings  # embedding
        ])

    collection.flush()
    print("✓ Columns ingestion completed")


def ingest_cells(docs):
    """Ingest cells data"""
    collection = create_cells_collection()

    print(f"→ Ingesting {len(docs)} cell documents")

    for chunk_start in range(0, len(docs), 64):
        batch = docs[chunk_start: chunk_start + 64]

        table_names = []
        column_names = []
        cell_values = []
        texts = []

        for doc in batch:
            metadata = doc.get("metadata", {})
            table_names.append(metadata.get("table_name", "unknown"))
            column_names.append(metadata.get("column_name", "unknown"))
            cell_values.append(doc.get("text", ""))
            texts.append(doc.get("text", ""))

        embeddings = get_ollama_embeddings_batch(texts, EMBED_MODEL)

        collection.insert([
            table_names,  # table_name
            column_names,  # column_name
            cell_values,  # cell_value
            embeddings  # embedding
        ])

    collection.flush()
    print("✓ Cells ingestion completed")


# --------------------------------------------------------
# Load and ingest data
# --------------------------------------------------------
def load_docs(path):
    if not os.path.exists(path):
        print(f"⚠ File not found: {path}")
        return []
    with open(path, "r") as f:
        return json.load(f)


# Load documents
tables_docs = load_docs(TABLES_PATH)
columns_docs = load_docs(COLUMNS_PATH)
cells_docs = load_docs(CELLS_PATH)

# Ingest data
if tables_docs:
    ingest_tables(tables_docs)

if columns_docs:
    ingest_columns(columns_docs)

if cells_docs:
    ingest_cells(cells_docs)

print("✓ All collections ingested successfully with correct schema")
