import time
from functools import wraps

from b2b_ec_utils.logger import get_logger

logger = get_logger("TimedRun")


def format_duration(seconds: float) -> str:
    """Format a duration given in seconds into a human-readable string.
    Args:
        seconds (float): Duration in seconds.
    Returns:
        str: Formatted duration string.
    """
    if seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    elif seconds < 60:
        return f"{seconds:.3f} s"
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours} h {minutes:02} m {seconds:02} s"


def timed_run(func):
    """Decorator to time the execution of a function and log its duration.
    Args:
        func (Callable): The function to be decorated.
    Returns:
        Callable: The wrapped function with timing.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_wall = time.perf_counter()
        start_cpu = time.process_time()
        logger.info("Started: %s", func.__name__)
        try:
            return func(*args, **kwargs)
        finally:
            wall = time.perf_counter() - start_wall
            cpu = time.process_time() - start_cpu
            logger.info("Completed %s in wall=%s cpu=%s", func.__name__, format_duration(wall), format_duration(cpu))

    return wrapper
