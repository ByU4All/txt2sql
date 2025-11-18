import os
from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class DatabaseSettings(BaseSettings):
    """Database configuration settings."""

    # Primary database
    db_uri: str = Field(..., alias="DATABASE_URI")
    db_host: str = Field(..., alias="DB_HOST")
    db_port: int = Field(5432, alias="DB_PORT")
    db_name: str = Field(..., alias="DB_NAME")
    db_user: str = Field(..., alias="DB_USER")
    db_password: str = Field(..., alias="DB_PASSWORD")

    # Neo4j graph database
    neo4j_uri: str = Field(..., alias="NEO4J_URI")
    neo4j_user: str = Field("neo4j", alias="NEO4J_USER")
    neo4j_password: str = Field(..., alias="NEO4J_PASSWORD")

    # Vector database (ChromaDB)
    chroma_host: str = Field("localhost", alias="CHROMA_HOST")
    chroma_port: int = Field(8000, alias="CHROMA_PORT")

class LLMSettings(BaseSettings):
    """LLM and API configuration."""

    openai_api_key: Optional[str] = Field(None, alias="OPENAI_API_KEY")
    openai_api_base_url: Optional[str] = Field(None, alias="OPENAI_API_BASE_URL")
    ollama_model: Optional[str] = Field(None, alias="OLLAMA_MODEL")
    anthropic_api_key: Optional[str] = Field(None, alias="ANTHROPIC_API_KEY")

    # Model configurations
    default_model: str = Field("gpt-4", alias="DEFAULT_LLM_MODEL")
    embedding_model: str = Field("text-embedding-ada-002", alias="EMBEDDING_MODEL")

    # API limits
    max_tokens: int = Field(4096, alias="MAX_TOKENS")
    temperature: float = Field(0.1, alias="LLM_TEMPERATURE")

class ApplicationSettings(BaseSettings):
    """General application settings."""

    # Environment
    environment: str = Field("development", alias="ENVIRONMENT")
    debug: bool = Field(False, alias="DEBUG")

    # Paths
    project_root: Path = Path(__file__).parent.parent.parent
    data_dir: Path = project_root / "data"
    schema_cache_dir: Path = data_dir / "schema_cache"
    sample_rows_dir: Path = data_dir / "sample_rows"
    examples_dir: Path = data_dir / "examples"

    # API settings
    api_host: str = Field("0.0.0.0", alias="API_HOST")
    api_port: int = Field(8000, alias="API_PORT")

    # Logging
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

class RetrievalSettings(BaseSettings):
    """Vector and graph retrieval settings."""

    # Vector search parameters
    vector_top_k: int = Field(10, alias="VECTOR_TOP_K")
    similarity_threshold: float = Field(0.7, alias="SIMILARITY_THRESHOLD")

    # Graph traversal limits
    max_join_depth: int = Field(3, alias="MAX_JOIN_DEPTH")
    max_tables_per_query: int = Field(5, alias="MAX_TABLES_PER_QUERY")

    # Context building
    max_context_length: int = Field(8000, alias="MAX_CONTEXT_LENGTH")
    include_sample_rows: bool = Field(True, alias="INCLUDE_SAMPLE_ROWS")
    max_sample_rows: int = Field(3, alias="MAX_SAMPLE_ROWS")

class ExecutionSettings(BaseSettings):
    """SQL execution and validation settings."""

    # Execution limits
    query_timeout: int = Field(30, alias="QUERY_TIMEOUT")  # seconds
    max_result_rows: int = Field(1000, alias="MAX_RESULT_ROWS")

    # Safety settings
    read_only_mode: bool = Field(True, alias="READ_ONLY_MODE")

    # Query refinement
    max_refinement_attempts: int = Field(3, alias="MAX_REFINEMENT_ATTEMPTS")

class Settings:
    """Main settings class that combines all configurations."""

    def __init__(self):
        self.database = DatabaseSettings()
        self.llm = LLMSettings()
        self.app = ApplicationSettings()
        self.retrieval = RetrievalSettings()
        self.execution = ExecutionSettings()

    def validate_required_settings(self) -> None:
        """Validate that all required settings are present."""
        required_checks = [
            (self.database.db_uri, "DATABASE_URI is required"),
            (self.database.neo4j_uri, "NEO4J_URI is required"),
            (self.llm.openai_api_key or self.llm.anthropic_api_key,
             "Either OPENAI_API_KEY or ANTHROPIC_API_KEY is required"),
        ]

        for value, message in required_checks:
            if not value:
                raise ValueError(message)

    def create_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        directories = [
            self.app.data_dir,
            self.app.schema_cache_dir,
            self.app.sample_rows_dir,
            self.app.examples_dir,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

# Global settings instance
settings = Settings()

# Constants used throughout the application
class Constants:
    """Application-wide constants."""

    # Collection names for vector store
    TABLES_COLLECTION = "table_embeddings"
    COLUMNS_COLLECTION = "column_embeddings"
    EXAMPLES_COLLECTION = "example_queries"

    # Schema metadata keys
    TABLE_DESCRIPTION_KEY = "description"
    COLUMN_TYPE_KEY = "data_type"
    FOREIGN_KEY_KEY = "foreign_keys"

    # Prompt templates
    SYSTEM_PROMPT_KEY = "system_prompt"
    USER_PROMPT_KEY = "user_prompt"
    EXAMPLES_PROMPT_KEY = "examples_prompt"

    # Error messages
    INVALID_QUERY_ERROR = "Generated SQL query is invalid"
    TIMEOUT_ERROR = "Query execution timeout"
    PERMISSION_ERROR = "Query not allowed in read-only mode"

# Initialize settings on import
try:
    settings.validate_required_settings()
    settings.create_directories()
except Exception as e:
    print(f"Warning: Settings validation failed: {e}")
