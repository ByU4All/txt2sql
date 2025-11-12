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

# TOOL USAGE
# $ list_of_tools = [tool1, tool2]
# $ model_with_tool = model.bind_tools(list_of_tools)