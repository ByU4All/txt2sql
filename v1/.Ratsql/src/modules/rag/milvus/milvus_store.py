from ..base import VectorStore

class MilvusStore(VectorStore):
    def __init__(self):
        super().__init__(host="localhost", port=8001, db_name="schema_embedding")
        raise NotImplementedError()
        self.collection_name = "v1"
        self.__user = "user"
        self.__password = "password"
        self.client = None  # placeholder for actual milvus client instance


    def connect(self):
        # Example connection logic
        print(f"Connecting to Milvus at {self.host}:{self.port} as {self.__user}")
        # self.client = Milvus(host=self.host, port=self.port, user=self.__user, password=self.__password)
        # return self.client

    def create_collection(self, name: str, **kwargs):
        print(f"Creating collection {name}")
        pass

    def drop_collection(self, name: str) -> None:
        print(f"Drop a collection {name}")
        pass

    def insert_embeddings(self, embeddings, metadata):
        print(f"Inserting {len(embeddings)} embeddings into {self.collection_name}")
        # self.client.insert(...)

    def delete_embeddings(self, name: str) -> None:
        print(f"Delete embeddings associated with the collection : {name}.")


    def query(self, vector, top_k=5):
        print(f"Querying top {top_k} similar vectors")
        # return self.client.search(...)
        return []

    @staticmethod
    def embed_data(data):
        print("Embedding data for MilvusStore")
        # Placeholder for embedding logic
        return [0.1, 0.2, 0.3]  # Example vector
