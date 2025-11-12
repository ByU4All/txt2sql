from langgraph.graph import MessagesState

class CustomState(MessagesState):
    """
    Custom state for the graph.
    """
    llm_calls : int
    resp : int
