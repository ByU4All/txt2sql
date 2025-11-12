from langgraph.graph import StateGraph, START, END

from states import (
    CustomState
)
from nodes import (
    node1
)
from utils import (
    llm_model,
    lf_handler,
    save_graph_to_file
)

graph_builder = StateGraph(CustomState)

graph_builder.add_node("node1", node1)

graph_builder.add_edge(START, "node1")
graph_builder.add_edge("node1", END)

compiled_graph = graph_builder.compile(checkpointer=False)

# Show the agent
# save_graph_to_file(runnable_graph=compiled_graph, output_file_path="graphs")
# Invoke

messages = [
    ("user", "what is 2 + 191"),
]

graph_run = compiled_graph.invoke(
    {
        "messages": messages,
    },
    config={
        "session_id": "flashing_lights",
        "user_id": "ye",
        "callbacks": [lf_handler]
    }
)
print(graph_run)

