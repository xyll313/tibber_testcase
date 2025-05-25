import pytest
from data_transformation.api_processor import APIProcessor
import requests
import pandas as pd
import unittest

# --- Pytest Fixture ---
@pytest.fixture
def api_processor():
    """Provides a fresh APIProcessor instance for each test."""
    return APIProcessor(base_url="http://mockapi.com")


def test_process_currencies_duplicate(api_processor, mocker):
    """Test process_currencies with successful API response."""
    mocker.patch.object(
        api_processor,
        "_get_currencies",
        return_value={
            "USD": {"name": "US Dollar", "symbol": "$"},
            "EUR": {"name": "Euro", "symbol": "€"},
            "USD": {"name": "US Dollar Duplicate", "symbol": "$$"},
        },
    )
    df = api_processor.process_currencies()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Should drop duplicate 'USD_duplicate'
    assert df.columns.tolist() == ["currency_code", "name", "symbol"]
    assert (
        df["currency_code"].dtype == "object" or df["currency_code"].dtype == "string"
    )
    assert df["name"].dtype == "object" or df["name"].dtype == "string"
    assert df["symbol"].dtype == "object" or df["symbol"].dtype == "string"

    expected_df = pd.DataFrame(
        {
            "currency_code": ["USD", "EUR"],
            "name": [
                "US Dollar Duplicate",
                "Euro",
            ],  # 'USD_duplicate' should overwrite 'USD' due to keep='last'
            "symbol": ["$$", "€"],
        }
    )
    pd.testing.assert_frame_equal(
        df.sort_values("currency_code").reset_index(drop=True),
        expected_df.sort_values("currency_code").reset_index(drop=True),
        check_dtype=False,
    )


def test_process_base_rates_success_duplicate(api_processor, mocker):
    """Test process_base_rates with successful API response."""
    mocker.patch.object(
        api_processor,
        "_get_base_rate",
        return_value={
            "date": "2023-04-01",
            "base": "USD",
            "rates": {"EUR": 0.92, "GBP": 0.80, "EUR": 0.93},  # Test duplicate handling
        },
    )
    df = api_processor.process_base_rates("USD")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2  # Should drop duplicate 'EUR_duplicate'
    assert df.columns.tolist() == ["currency_code", "rate", "date"]
    assert df["date"].iloc[0] == "2023-04-01"

    expected_df = pd.DataFrame(
        {
            "currency_code": ["EUR", "GBP"],
            "rate": [0.93, 0.80],  # 'EUR_duplicate' should overwrite 'EUR'
            "date": ["2023-04-01", "2023-04-01"],
        }
    )
    pd.testing.assert_frame_equal(
        df.sort_values("currency_code").reset_index(drop=True),
        expected_df.sort_values("currency_code").reset_index(drop=True),
    )


def test_process_base_rates_NOK_success(api_processor, mocker):
    """Test process_base_rates_NOK with successful API response."""
    mocker.patch.object(
        api_processor,
        "_get_base_rate",
        return_value={
            "date": "2023-05-10",
            "base": "NOK",
            "rates": {"USD": 0.095, "EUR": 0.085},
        },
    )
    df = api_processor.process_base_rates_NOK()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.columns.tolist() == [
        "target_currency_code",
        "rate",
        "date",
        "base_currency_code",
    ]
    assert df["base_currency_code"].iloc[0] == "NOK"
    assert df["date"].iloc[0] == "2023-05-10"

    expected_df = pd.DataFrame(
        {
            "target_currency_code": ["USD", "EUR"],
            "rate": [0.095, 0.085],
            "date": ["2023-05-10", "2023-05-10"],
            "base_currency_code": ["NOK", "NOK"],
        }
    )
    pd.testing.assert_frame_equal(
        df.sort_values("target_currency_code").reset_index(drop=True),
        expected_df.sort_values("target_currency_code").reset_index(drop=True),
    )


def test_process_hist_rates_success_duplicates(api_processor, mocker):
    """Test process_hist_rates with successful API response."""
    mocker.patch.object(
        api_processor,
        "_get_historical_rate",
        return_value={
            "date": "2022-11-15",
            "base": "EUR",
            "rates": {"USD": 1.05, "NOK": 10.5, "USD": 1.06},
        },
    )
    df = api_processor.process_hist_rates("2022-11-15")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.columns.tolist() == ["currency_code", "rate_EUR", "date"]
    assert df["date"].iloc[0] == "2022-11-15"

    expected_df = pd.DataFrame(
        {
            "currency_code": ["USD", "NOK"],
            "rate_EUR": [1.06, 10.5],
            "date": ["2022-11-15", "2022-11-15"],
        }
    )
    pd.testing.assert_frame_equal(
        df.sort_values("currency_code").reset_index(drop=True),
        expected_df.sort_values("currency_code").reset_index(drop=True),
    )


def test_process_hist_rate_NOK_success(api_processor, mocker):
    """Test process_hist_rate_NOK with successful historical rates."""
    # Mock the output of process_hist_rates (which returns EUR-based rates)
    mocker.patch.object(
        api_processor,
        "process_hist_rates",
        return_value=pd.DataFrame(
            {
                "currency_code": ["USD", "EUR", "NOK", "GBP"],
                "rate_EUR": [1.08, 1.00, 10.00, 0.85],  # NOK rate is 10.00
                "date": ["2023-03-01"] * 4,
            }
        ),
    )

    df = api_processor.process_hist_rate_NOK("2023-03-01")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 4  # All rows should be processed
    assert df.columns.tolist() == [
        "target_currency_code",
        "date",
        "base_currency_code",
        "rate",
    ]
    assert df["base_currency_code"].iloc[0] == "NOK"

    expected_df = pd.DataFrame(
        {
            "target_currency_code": ["USD", "EUR", "NOK", "GBP"],
            "date": ["2023-03-01"] * 4,
            "base_currency_code": ["NOK"] * 4,
            "rate": [0.108, 0.100, 1.000, 0.085],
        }
    )
    pd.testing.assert_frame_equal(
        df.sort_values("target_currency_code").reset_index(drop=True),
        expected_df.sort_values("target_currency_code").reset_index(drop=True),
    )
