import os
from langchain_ollama import ChatOllama
from langfuse import get_client, Langfuse
from langfuse.langchain import CallbackHandler

LF_SECRET_KEY = "sk-lf-3d15d46b-46b1-46ba-9747-6e68d73e3125"
LF_PUBLIC_KEY = "sk-lf-3d15d46b-46b1-46ba-9747-6e68d73e3125"
LF_HOST = "https://cloud.langfuse.com"
Langfuse(
    public_key=LF_PUBLIC_KEY,
    secret_key=LF_SECRET_KEY,
    host=LF_HOST
)
# Initialize Langfuse client
langfuse = get_client()

# Initialize Langfuse CallbackHandler for Langchain (tracing)
langfuse_handler = CallbackHandler()
from langchain.tools import tool

@tool
def addition_tool(arg1, arg2):
    """
    This tool performs addition between two numbers.
    Args:
        arg1: first number
        arg2: second number
    """
    print("inside addition tool")
    return arg1 + arg2

@tool
def subtraction_tool(arg1, arg2):
    """
    This tool performs subtraction between two numbers.
    Args:
        arg1: first number
        arg2: second number
    """
    print("inside subtraction tool")
    return arg1 - arg2

llm = ChatOllama(
    model="qwen2.5:0.5b",
    temperature=0,
)

messages = [
    (
        "system",
        """You're a assistance tasked to answer the user question.\
Use the tools if needed to generate the answer the question.\
Once you have the answer return the answer to the user and explain why and what tool you used to generate the answer.
""",
    ),
    ("human", "what is 2 + 1"),
]
messages = {
    "messages": [
        {
            "role": "system",
            "content": """You're a assistance tasked to answer the user question.\
Use the tools if needed to generate the answer the question.\
Once you have the answer return the answer to the user and explain why and what tool you used to generate the answer.
"""
        },
        {
            "role": "user",
            "content": "What is 1 + 299"
        }
    ]
}

# llm = llm.bind_tools([addition_tool, subtraction_tool])
from langchain.agents import create_agent
llm = create_agent(
    model=llm,
    tools=[addition_tool, subtraction_tool]
)
# ai_msg = llm.invoke(messages, config={"callbacks": [langfuse_handler]})
ai_msg = llm.invoke(messages)
print(ai_msg)