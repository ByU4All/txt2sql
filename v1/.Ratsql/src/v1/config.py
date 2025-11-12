from langfuse import get_client, Langfuse
from langfuse.langchain import CallbackHandler





class Config:
    def __init__(self):
        self.model_name = "granite4:350m"
        self.temperature = 0
        self.__LF_SECRET_KEY = "sk-lf-3d15d46b-46b1-46ba-9747-6e68d73e3125"
        self.__LF_PUBLIC_KEY = "sk-lf-3d15d46b-46b1-46ba-9747-6e68d73e3125"
        self.__LF_HOST = "https://cloud.langfuse.com"

    def langfuse_handler(self):
        Langfuse(
            public_key=self.__LF_PUBLIC_KEY,
            secret_key=self.__LF_SECRET_KEY,
            host=self.__LF_HOST
        )
        # Initialize Langfuse client
        langfuse = get_client()

        # Initialize Langfuse CallbackHandler for Langchain (tracing)
        langfuse_handler = CallbackHandler()
        return langfuse_handler


node1_prompt = """
You're a assistance tasked to answer the user question.
Use the tools if needed to generate the answer the question.
Once you have the answer return the answer to the user and explain why and what tool you used to generate the answer.
"""