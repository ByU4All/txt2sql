from abc import ABC, abstractmethod
from typing import Any, List, Dict

class VectorStore(ABC):
    """
    Abstract base class for vector store integrations.
    """

    def __init__(self, host: str = None, port: int = None, db_name: str = None):
        self.host = host
        self.port = port
        self.db_name = db_name

    @abstractmethod
    def connect(self) -> None:
        """Connect to the vector database."""
        pass

    @abstractmethod
    def create_collection(self, name: str, **kwargs) -> None:
        """Create a collection or index."""
        pass

    @abstractmethod
    def drop_collection(self, name: str) -> None:
        """Drop a collection or index."""
        pass

    @abstractmethod
    def insert_embeddings(self, embeddings: List[List[float]], metadata: List[Dict[str, Any]]) -> None:
        """Insert embeddings and metadata into the collection."""
        pass

    @abstractmethod
    def delete_embeddings(self, name: str) -> None:
        """Delete embeddings associated with the collection."""
        pass

    @abstractmethod
    def query(self, vector: List[float], top_k: int = 5) -> List[Dict[str, Any]]:
        """Query the store to find similar vectors."""
        pass

    @staticmethod
    @abstractmethod
    def embed_data(data: Any) -> List[float]:
        """Generate embeddings for the given data."""
        pass
