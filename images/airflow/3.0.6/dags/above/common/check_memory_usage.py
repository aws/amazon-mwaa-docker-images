import psutil


def check_memory_usage(tag: str = "", verbose: bool=True) -> float:
    """
    Check and print current memory usage percentage.

    Args:
        tag (str): Optional tag to print alongside memory usage for context.
        verbose (bool): If True, only the memory usage percentage is printed.
    Returns:
        float: The current memory usage percentage.
    """
    prefix: str = ""
    if tag:
        prefix = f"*** :: {tag} :: "
        
    # Get memory information
    memory = psutil.virtual_memory()

    # Print memory statistics
    print(f"{prefix}Memory Usage: {memory.percent}%")
    if not verbose:
        print(f"{prefix}Total Memory: {memory.total / (1024**3):.2f} GB")
        print(f"{prefix}Available Memory: {memory.available / (1024**3):.2f} GB")
        print(f"{prefix}Used Memory: {memory.used / (1024**3):.2f} GB")
        print(f"{prefix}Free Memory: {memory.free / (1024**3):.2f} GB")

    return memory.percent
