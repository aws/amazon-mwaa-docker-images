"""
Contain various utility classes and functions.
"""


def qualified_name(cls: type) -> str:
    """
    Return the fully qualified name of a class.

    :param cls - The class to return its qualified name.

    :returns The fully qualified name of a class.
    """
    module = cls.__module__
    qualname = cls.__qualname__
    return f"{module}.{qualname}"
