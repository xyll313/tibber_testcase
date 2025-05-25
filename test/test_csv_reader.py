import pytest
import os
import pandas as pd
import datetime
from datetime import timezone
from unittest.mock import MagicMock
from data_transformation.csv_reader import CsvProcessor


@pytest.fixture
def mock_db_connector():
    """Provides a mock database connector with a mock upsert method."""
    connector = MagicMock()
    connector.upsert_record_item_prices = MagicMock()
    return connector


@pytest.fixture
def csv_processor(mock_db_connector, mocker):
    """Provides a CsvProcessor instance with mocked path dependencies."""
    # Mock os.path.abspath(__file__) and os.path.dirname for consistent path calculation
    mocker.patch("os.path.abspath", return_value="/mock/path/script/test_script.py")
    mocker.patch("os.path.dirname", return_value="/mock/path/script")
    # The related_batch_dir_to_script should be relative to the mocked script directory
    # so "../batch_data" will resolve to "/mock/path/batch_data"
    processor = CsvProcessor(
        mock_db_connector, related_batch_dir_to_script="../batch_data"
    )
    return processor


def test_get_batch_number_valid(csv_processor):
    """Test _get_batch_number with valid filenames."""
    assert csv_processor._get_batch_number("batch1.csv") == 1
    assert csv_processor._get_batch_number("batch123.csv") == 123
    assert csv_processor._get_batch_number("data_batch_99.csv") == 99


def test_get_batch_number_invalid(csv_processor):
    """Test _get_batch_number with invalid filenames."""
    assert csv_processor._get_batch_number("nobatch.csv") == float("inf")
    assert csv_processor._get_batch_number("batch.csv") == float("inf")
    assert csv_processor._get_batch_number("not_a_csv.txt") == float("inf")
    assert csv_processor._get_batch_number("") == float("inf")


def test_order_csvs(csv_processor, mocker):
    """Test _order_csvs to ensure correct sorting and filtering."""
    mock_listdir = mocker.patch(
        "os.listdir",
        return_value=["batch3.csv", "batch1.csv", "other_file.csv", "batch2.csv",],
    )
    # Set a predictable batch_data_dir in the mock object
    csv_processor.batch_data_dir = "/mock/batch/data"

    ordered_list = csv_processor._order_csvs()
    assert ordered_list == ["batch1.csv", "batch2.csv", "batch3.csv", "other_file.csv"]


def test_process_csvs_basic_flow(csv_processor, mocker):
    """Test basic processing with some new and some old rows."""
    # Mock file system and CSV content
    mocker.patch("os.listdir", return_value=["batch1.csv", "batch2.csv"])
    mocker.patch(
        "os.path.join", side_effect=lambda a, b: f"{a}/{b}"
    )  # Mock join for predictable paths
    csv_processor.batch_data_dir = (
        "/mock/batch_data"  # Set a predictable batch_data_dir
    )

    # Mock the content of batch1.csv
    df1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "item": ["A", "B", "C"],
            "price": [10.0, 20.0, 30.0],
            "currency": ["USD", "EUR", "GBP"],
            "created_at": [
                "2023-01-01T10:00:00Z",
                "2023-01-01T11:00:00Z",
                "2023-01-01T12:00:00Z",
            ],
            "updated_at": [
                "2023-01-01T10:00:00Z",
                "2023-01-01T11:00:00Z",
                "2023-01-01T12:00:00Z",
            ],
            "system_timestamp": [
                "2023-01-01T10:00:00Z",
                "2023-01-01T11:00:00Z",
                "2023-01-01T12:00:00Z",
            ],
        }
    )
    # Mock the content of batch2.csv
    df2 = pd.DataFrame(
        {
            "id": [4, 5],
            "item": ["D", "E"],
            "price": [40.0, 50.0],
            "currency": ["JPY", "AUD"],
            "created_at": ["2023-01-01T13:00:00Z", "2023-01-01T14:00:00Z"],
            "updated_at": ["2023-01-01T13:00:00Z", "2023-01-01T14:00:00Z"],
            "system_timestamp": ["2023-01-01T13:00:00Z", "2023-01-01T14:00:00Z"],
        }
    )

    # Configure mock_read_csv to return different DataFrames for different files
    mock_read_csv = mocker.patch("pandas.read_csv", side_effect=[df1, df2])

    # Set initial checkpoint to filter out some rows
    csv_processor.check_point = datetime.datetime(
        2023, 1, 1, 11, 30, 0, tzinfo=timezone.utc
    )

    # Execute the method
    returned_dates = csv_processor.process_csvs()

    # Assertions
    # upsert_record_item_prices should be called only for rows after the checkpoint
    # Row 3 from df1, Row 4, 5 from df2
    assert csv_processor.db_connector.upsert_record_item_prices.call_count == 3

    # Check calls for df1 (row 3, id=3)
    csv_processor.db_connector.upsert_record_item_prices.assert_any_call(
        3,
        "C",
        30.0,
        "GBP",
        pd.Timestamp("2023-01-01 12:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-01-01 12:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-01-01 12:00:00+0000", tz="UTC"),
    )
    # Check calls for df2 (rows 4, 5)
    csv_processor.db_connector.upsert_record_item_prices.assert_any_call(
        4,
        "D",
        40.0,
        "JPY",
        pd.Timestamp("2023-01-01 13:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-01-01 13:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-01-01 13:00:00+0000", tz="UTC"),
    )
    csv_processor.db_connector.upsert_record_item_prices.assert_any_call(
        5,
        "E",
        50.0,
        "AUD",
        pd.Timestamp("2023-01-01 14:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-01-01 14:00:00+0000", tz="UTC"),
        pd.Timestamp("2023-01-01 14:00:00+0000", tz="UTC"),
    )

    # Verify final checkpoint
    assert csv_processor.check_point == pd.Timestamp(
        "2023-01-01 14:00:00+0000", tz="UTC"
    )

    # Verify returned date_list (unique dates from updated_at)
    expected_dates = [datetime.date(2023, 1, 1)]  # All dates are 2023-01-01
    assert sorted(returned_dates) == sorted(
        expected_dates
    )  # Use sorted for list comparison
