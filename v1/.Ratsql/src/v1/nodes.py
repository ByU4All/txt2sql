from typing import Literal

from langchain.messages import SystemMessage
from langgraph.types import Command, StreamWriter
from states import CustomState
from utils import llm_model
from tools import addition_tool, subtraction_tool
from config import node1_prompt
from langchain.agents import create_agent

def node1(state: CustomState, writer: StreamWriter) -> Command[Literal["__end__"]]:
    writer({"process": "performing task inside node1"})
    state_to_update = {}
    # llm = llm_model.bind_tools([addition_tool, subtraction_tool])
    llm = create_agent(llm_model, tools=[addition_tool, subtraction_tool])
    resp = llm.invoke(
        [
            {
                "role": "system",
                "content": node1_prompt
            },
            {
                "role": "user",
                "content": state["messages"][-1]
            }
        ]
    )

    state_to_update["resp"] = resp
    state_to_update["messages"] = resp
    return Command(
        update= state_to_update,
        goto="__end__"
    )
