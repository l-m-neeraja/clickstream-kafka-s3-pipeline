from datetime import datetime
import pandas as pd

REQUIRED_FIELDS = {"user_id", "event_type", "timestamp"}


def clean_and_transform_event(event: dict) -> dict | None:
    """
    Validate and normalize a single clickstream event.
    Returns transformed event or None if invalid.
    """
    if not REQUIRED_FIELDS.issubset(event):
        return None

    try:
        event["event_type"] = event["event_type"].lower()
        event["timestamp"] = datetime.fromisoformat(event["timestamp"])
        event["ingestion_timestamp"] = datetime.utcnow()
        return event
    except Exception:
        return None


def batch_process_data(records: list[dict]) -> pd.DataFrame:
    """
    Convert a list of transformed events into a Pandas DataFrame.
    """
    return pd.DataFrame(records)
