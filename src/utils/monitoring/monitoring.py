import logging
import time
import os
from contextlib import contextmanager
from datetime import datetime
import json
import threading
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Global dictionary to store query metrics
_query_metrics = {
    "metrics": []
}

# Lock for thread safety when updating metrics
_metrics_lock = threading.Lock()


def _add_metric(name: str, duration_ms: float, query_type: str, details: Optional[Dict] = None):
    """
    Add a metric entry to the global metrics dictionary.

    Args:
        name (str): Name or identifier of the query/operation
        duration_ms (float): Duration in milliseconds
        query_type (str): Type of query or operation
        details (Dict, optional): Additional details about the operation
    """
    with _metrics_lock:
        timestamp = datetime.now().isoformat()
        metric = {
            "name": name,
            "timestamp": timestamp,
            "duration_ms": duration_ms,
            "type": query_type
        }

        if details:
            metric["details"] = details

        _query_metrics["metrics"].append(metric)


@contextmanager
def monitor_query_performance(name: str, query_type: str = "spark_operation"):
    """
    Context manager to monitor and record the performance of a query or operation.

    Args:
        name (str): Name of the operation to monitor
        query_type (str): Type of query or operation

    Example:
        with monitor_query_performance("daily_top_ips_query", "analytics"):
            # Run your query here
    """
    start_time = time.time()
    try:
        logger.info(f"Starting operation: {name}")
        yield
    finally:
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000  # Convert to milliseconds
        _add_metric(name, duration_ms, query_type)
        logger.info(f"Completed operation: {name} in {duration_ms:.2f}ms")


def save_metrics_to_file(file_path: str = "query_metrics.json"):
    """
    Save the collected metrics to a JSON file.

    Args:
        file_path (str): Path to save the metrics file
    """
    with _metrics_lock:
        with open(file_path, 'w') as f:
            json.dump(_query_metrics, f, indent=2)

    logger.info(f"Metrics saved to {file_path}")


def get_metrics_summary():
    """
    Get a summary of the collected metrics.

    Returns:
        Dict: Summary of metrics including total operations, average duration, etc.
    """
    with _metrics_lock:
        metrics = _query_metrics["metrics"]

        if not metrics:
            return {"total_operations": 0, "message": "No metrics collected"}

        total_duration = sum(m["duration_ms"] for m in metrics)
        avg_duration = total_duration / len(metrics)

        # Group by type
        grouped_by_type = {}
        for metric in metrics:
            query_type = metric["type"]
            if query_type not in grouped_by_type:
                grouped_by_type[query_type] = []
            grouped_by_type[query_type].append(metric["duration_ms"])

        type_summaries = {}
        for query_type, durations in grouped_by_type.items():
            type_summaries[query_type] = {
                "count": len(durations),
                "avg_duration_ms": sum(durations) / len(durations),
                "min_duration_ms": min(durations),
                "max_duration_ms": max(durations)
            }

        return {
            "total_operations": len(metrics),
            "total_duration_ms": total_duration,
            "avg_duration_ms": avg_duration,
            "by_type": type_summaries
        }


def print_metrics_summary():
    """
    Print a summary of metrics to the log.
    """
    summary = get_metrics_summary()
    logger.info("=== Query Performance Metrics Summary ===")
    logger.info(f"Total operations: {summary['total_operations']}")

    if summary['total_operations'] > 0:
        logger.info(f"Average duration: {summary['avg_duration_ms']:.2f}ms")
        logger.info("Performance by operation type:")

        for query_type, type_summary in summary.get('by_type', {}).items():
            logger.info(f"  {query_type}: {type_summary['count']} operations, "
                        f"avg: {type_summary['avg_duration_ms']:.2f}ms, "
                        f"min: {type_summary['min_duration_ms']:.2f}ms, "
                        f"max: {type_summary['max_duration_ms']:.2f}ms")

    logger.info("=======================================")