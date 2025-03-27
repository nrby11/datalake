import logging
import time
logger = logging.getLogger(__name__)


def timeit(method):
    """Decorator to measure and log the execution time of a method."""
    def timed(*args, **kwargs):
        start_time = time.time()
        result = method(*args, **kwargs)
        elapsed_time = time.time() - start_time
        logger.info(f"Method {method.__name__} took {elapsed_time:.2f} seconds")
        return result
    return timed
