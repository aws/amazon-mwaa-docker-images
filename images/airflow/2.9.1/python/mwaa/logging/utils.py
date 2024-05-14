# Python imports
from functools import wraps
from typing import Callable, Any, TypeVar
import time


def parse_arn(log_group_arn: str):
    try:
        split_arn = log_group_arn.split(":")
        log_group = split_arn[6]
        region_name = split_arn[3] if split_arn[3] else None

        return log_group, region_name
    except Exception as ex:
        raise RuntimeError(f"Invalid log group ARN: {log_group_arn}") from ex


F = TypeVar("F", bound=Callable[..., Any])


def throttle(
    seconds: float,
    log_throttling_msg: bool = False,
) -> Callable[[F], F]:
    """
    A decorator that limits the rate at which a function can be called. If the function
    is called more than once within the specified number of seconds, it will be
    throttled and will not execute again until the time limit has passed.

    Args:
        seconds (float): The number of seconds to wait between function calls.
        log_throttling_msg (bool): If true, a message will be printed in case the call
          to the function gets throttled. If false, the function will be silently
          throttled. You probably want to set this to True if a function is not called
          frequently, and thus will not result in log pollution. However, for a functions
          that gets called fairly regularly, it is better to set this to False.

    Returns:
        Callable[[F], F]: A decorated function that will enforce the throttling.

    Example:
        @throttle(5)
        def my_function():
            print("Function executed!")

        my_function()  # Function executed!
        my_function()  # Throttling: please wait 4.98 more seconds before calling my_function.
    """

    def decorator(func: F) -> F:
        last_called = [0.0]

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_time = time.time()  # Get the current time
            elapsed = current_time - last_called[0]  # Calculate time since last call
            if elapsed < seconds:
                wait_time = seconds - elapsed
                if log_throttling_msg:
                    print(
                        f"Throttling {func.__name__} for {wait_time:.2f} more seconds."
                    )
                return None
            else:
                last_called[0] = current_time  # Update the last called time
                return func(*args, **kwargs)  # Call the original function

        return wrapper  # type: ignore

    return decorator
