```
text2sql_project/
│
├── README.md
├── requirements.txt
├── pyproject.toml              # optional if using poetry/pipenv
├── .env                        # DB and API credentials
├── .gitignore
│
├── data/                       # static or cached data
│   ├── schema_cache/           # exported DB schema JSONs
│   ├── sample_rows/            # sampled cell values
│   └── examples/               # curated Q→SQL pairs
│
├── notebooks/                  # for experimentation
│   ├── prototype_vector_search.ipynb
│   └── schema_visualization.ipynb
│
├── src/
│   ├── __init__.py
│   │
│   ├── config/                 # envs + settings
│   │   ├── __init__.py
│   │   └── settings.py         # API keys, DB URIs, constants
│   │
│   ├── schema/                 # extraction + sync jobs
│   │   ├── __init__.py
│   │   ├── extractor.py        # pull schema via SQLAlchemy
│   │   ├── vector_builder.py   # build embeddings for tables/columns
│   │   └── graph_loader.py     # push schema into Neo4j
│   │
│   ├── retrieval/              # vector + graph retrieval
│   │   ├── __init__.py
│   │   ├── vector_retriever.py # semantic recall
│   │   ├── graph_connector.py  # FK traversal + join inference
│   │   └── hybrid_retriever.py # combines both retrievals
│   │
│   ├── context/                # builds LLM context
│   │   ├── __init__.py
│   │   ├── builder.py          # light schema + join text
│   │   └── examples_retriever.py
│   │
│   ├── generator/              # LLM interaction
│   │   ├── __init__.py
│   │   ├── prompts.yaml
│   │   ├── generator.py        # ICL + reasoning generation
│   │   └── fixer.py            # syntax/semantic fixers
│   │
│   ├── execution/              # run + validate SQL
│   │   ├── __init__.py
│   │   ├── executor.py         # safe DB execution
│   │   ├── refiner.py          # invalid query correction
│   │   └── selector.py         # scoring + selection
│   │
│   ├── pipeline/               # orchestration layer
│   │   ├── __init__.py
│   │   ├── main_pipeline.py    # query → SQL full chain
│   │   ├── api.py              # FastAPI/Flask endpoint
│   │   └── logger.py           # telemetry + audit logging
│   │
│   └── jobs/                   # recurring maintenance
│       ├── __init__.py
│       ├── schema_sync.py      # refresh graph + vector data
│       ├── embedding_refresh.py
│       └── cleanup.py
│
├── tests/                      # pytest suite
│   ├── test_retriever.py
│   ├── test_graph_connector.py
│   ├── test_generator.py
│   ├── test_executor.py
│   └── fixtures/
│       ├── sample_schema.json
│       └── dummy_query_examples.json
│
├── scripts/                    # utility scripts & CLIs
│   ├── ingest_schema.sh
│   ├── run_server.sh
│   └── export_graph_snapshot.sh
│
└── docker/                     # deployment assets
    ├── Dockerfile
    ├── docker-compose.yml
    ├── neo4j/
    │   ├── Dockerfile
    │   └── config/
    └── chroma/
        ├── Dockerfile
        └── config/

```