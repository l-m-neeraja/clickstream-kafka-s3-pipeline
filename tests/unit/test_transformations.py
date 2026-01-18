from src.transformations import clean_and_transform_event, batch_process_data
from datetime import datetime


def test_valid_event_transformation():
    event = {
        "user_id": "123",
        "event_type": "VIEW_PRODUCT",
        "timestamp": "2026-01-18T10:00:00"
    }

    result = clean_and_transform_event(event)

    assert result is not None
    assert result["event_type"] == "view_product"
    assert isinstance(result["timestamp"], datetime)
    assert "ingestion_timestamp" in result


def test_missing_required_field():
    event = {
        "event_type": "view_product",
        "timestamp": "2026-01-18T10:00:00"
    }

    result = clean_and_transform_event(event)

    assert result is None


def test_batch_process_data():
    records = [
        {
            "user_id": "1",
            "event_type": "view",
            "timestamp": datetime.utcnow(),
            "ingestion_timestamp": datetime.utcnow()
        }
    ]

    df = batch_process_data(records)

    assert not df.empty
    assert len(df) == 1
