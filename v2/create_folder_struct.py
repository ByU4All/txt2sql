import os
from pathlib import Path

base = Path(".")

folders = [
    "data/schema_cache",
    "data/sample_rows",
    "data/examples",
    "notebooks",
    "src/config",
    "src/schema",
    "src/retrieval",
    "src/context",
    "src/generator",
    "src/execution",
    "src/pipeline",
    "src/jobs",
    "tests/fixtures",
    "scripts",
    "docker",
    "docker/neo4j",
    "docker/neo4j/config",
    "docker/chroma",
    "docker/chroma/config",
]

files = [
    "README.md",
    "requirements.txt",
    "pyproject.toml",
    ".env",
    ".gitignore",
    "data/schema_cache/.keep",
    "data/sample_rows/.keep",
    "data/examples/.keep",
    "notebooks/prototype_vector_search.ipynb",
    "notebooks/schema_visualization.ipynb",
    "src/__init__.py",
    "src/config/__init__.py",
    "src/config/settings.py",
    "src/schema/__init__.py",
    "src/schema/extractor.py",
    "src/schema/vector_builder.py",
    "src/schema/graph_loader.py",
    "src/retrieval/__init__.py",
    "src/retrieval/vector_retriever.py",
    "src/retrieval/graph_connector.py",
    "src/retrieval/hybrid_retriever.py",
    "src/context/__init__.py",
    "src/context/builder.py",
    "src/context/examples_retriever.py",
    "src/generator/__init__.py",
    "src/generator/prompts.yaml",
    "src/generator/generator.py",
    "src/generator/fixer.py",
    "src/execution/__init__.py",
    "src/execution/executor.py",
    "src/execution/refiner.py",
    "src/execution/selector.py",
    "src/pipeline/__init__.py",
    "src/pipeline/main_pipeline.py",
    "src/pipeline/api.py",
    "src/pipeline/logger.py",
    "src/jobs/__init__.py",
    "src/jobs/schema_sync.py",
    "src/jobs/embedding_refresh.py",
    "src/jobs/cleanup.py",
    "tests/test_retriever.py",
    "tests/test_graph_connector.py",
    "tests/test_generator.py",
    "tests/test_executor.py",
    "tests/fixtures/sample_schema.json",
    "tests/fixtures/dummy_query_examples.json",
    "scripts/ingest_schema.sh",
    "scripts/run_server.sh",
    "scripts/export_graph_snapshot.sh",
    "docker/Dockerfile",
    "docker/docker-compose.yml",
    "docker/neo4j/Dockerfile",
    "docker/chroma/Dockerfile",
]

for folder in folders:
    (base / folder).mkdir(parents=True, exist_ok=True)

for file in files:
    file_path = base / file
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.touch(exist_ok=True)
