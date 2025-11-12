from langchain_ollama import ChatOllama
from config import Config


graph_config = Config()

llm_model = ChatOllama(
    model=graph_config.model_name,
    temperature=graph_config.temperature,
)

lf_handler = graph_config.langfuse_handler()

def save_graph_to_file(runnable_graph, output_file_path):
    file_path = f"{output_file_path}.png"
    png_bytes = runnable_graph.get_graph().draw_mermaid_png()
    with open(file_path, 'wb') as file:
        file.write(png_bytes)