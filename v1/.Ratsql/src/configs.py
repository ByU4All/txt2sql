from langfuse import get_client, Langfuse
from langfuse.langchain import CallbackHandler
from langchain_ollama import ChatOllama

class LangfuseConfig:
    def __init__(self):
        self.__SECRET_KEY = "sk-lf-3d15d46b-46b1-46ba-9747-6e68d73e3125"
        self.__PUBLIC_KEY = "sk-lf-3d15d46b-46b1-46ba-9747-6e68d73e3125"
        self.__HOST = "https://cloud.langfuse.com"

    def langfuse_handler(self):
        Langfuse(
            public_key=self.__PUBLIC_KEY,
            secret_key=self.__SECRET_KEY,
            host=self.__HOST
        )
        langfuse = get_client()
        langfuse_handler = CallbackHandler()
        return langfuse_handler

class OllamaConfig:
    def __init__(self):
        self.model_name = "granite4:350m"
        self.temperature = 0

    def chat_model(self):
        llm_model = ChatOllama(
            model=self.model_name,
            temperature=self.temperature
        )
        return llm_model