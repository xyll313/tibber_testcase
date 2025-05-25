import requests
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Numeric, Date, String, Table


class APIProcessor:
    """
    A client for interacting with the Vatcomply API.
    Provides methods to retrieve currency information and exchange rates.
    """

    def __init__(self, base_url="https://api.vatcomply.com"):
        self.base_url = base_url

    def _make_request(self, endpoint: str, params=None):
        """
        Internal helper method to make an API call and handle common error checking.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error making API call to {url}: {e}")
            return None

    def _get_currencies(self):
        """
        Makes an API call to /currencies to get a list of all supported currencies.

        Returns:
            dict: A JSON response with all currencies, or None if an error occurs.
        """
        return self._make_request("/currencies")

    def _get_base_rate(self, base_currency: str):
        """
        Makes an API call to /rates?base={base_currency} to get exchange rates
        relative to a specified base currency.

        Args:
            base_currency (str): The three-letter currency code (e.g., "USD", "EUR").

        Returns:
            dict: A JSON response with exchange rates, or None if an error occurs.
        """
        # Note: Removed the space after 'base=' from your original URL, as it might cause issues.
        return self._make_request("/rates", params={"base": base_currency.upper()})

    def _get_historical_rate(self, date: str):
        """
        Makes an API call to /rates?date={date} to get historical exchange rates.
        NOTE: EUR is the base currency for historical rates, and further conversion
        would be required if you need a different base.

        Args:
            date (str): The date in 'YYYY-MM-DD' format (e.g., "2023-01-15").

        Returns:
            dict: A JSON response with historical exchange rates, or None if an error occurs.
        """
        return self._make_request("/rates", params={"date": date})

    def process_currencies(self):
        """
        calls _get_currencies to receive a json then create a dataframe that ready to be load into DB

        Return:
            pd.DataFrame: a dataframe object with processed data with consistent data format as in DB
        """
        currencies_json = self._get_currencies()
        df_init = pd.DataFrame.from_dict(currencies_json, orient="index")
        df_processed = df_init.reset_index().rename(columns={"index": "currency_code"})
        df_final = df_processed.drop_duplicates(subset=["currency_code"], keep="last")
        df_final["currency_code"] = df_final["currency_code"].astype(str)
        df_final["name"] = df_final["name"].astype(str)
        df_final["symbol"] = df_final["symbol"].astype(str)
        return df_final

    def process_base_rates(self, base_currency: str):
        """
        calls _get_base_rate to receive a json then create a dataframe that ready to be load into DB

        Return:
            pd.DataFrame: a dataframe object with processed data
        """
        base_rates_json = self._get_base_rate(base_currency)
        date = base_rates_json["date"]
        df = (
            pd.DataFrame.from_dict(base_rates_json["rates"], orient="index")
            .reset_index()
            .rename(columns={"index": "currency_code", 0: "rate"})
        )
        df["date"] = date
        df_final = df.drop_duplicates(subset=["currency_code"], keep="last")
        return df_final

    def process_base_rates_NOK(self):
        """
        calls _get_base_rate with NOK as base_rate currency

        Return:
            pd.DataFrame: a dataframe object with processed data
        """
        base_rates_json = self._get_base_rate("NOK")
        date = base_rates_json["date"]
        df = (
            pd.DataFrame.from_dict(base_rates_json["rates"], orient="index")
            .reset_index()
            .rename(columns={"index": "target_currency_code", 0: "rate"})
        )
        df_final = df.drop_duplicates(subset=["target_currency_code"], keep="last")
        df_final["date"] = date
        df_final["base_currency_code"] = "NOK"
        return df_final

    def process_hist_rates(self, date: str):
        """
        calls _get_hist_rate to receive historical currency conversion rates

        Return:
            pd.DataFrame: a dataframe object with processed data
        """
        output_json = self._get_historical_rate(date)
        date = output_json["date"]
        df = (
            pd.DataFrame.from_dict(output_json["rates"], orient="index")
            .reset_index()
            .rename(columns={"index": "currency_code", 0: "rate_EUR"})
        )
        df["date"] = date
        df_processed = df.drop_duplicates(subset=["currency_code"], keep="last")
        return df_processed

    def process_hist_rate_NOK(self, date: str):
        """
        calls process_hist_rates to receive historical currency conversion rates
        Calculate rates with NOK as base currency

        Return:
            pd.DataFrame: a dataframe object with processed data
        """
        df = self.process_hist_rates(date)
        df["base_currency_code"] = "NOK"
        nok_rate_eur = df[df["currency_code"] == "NOK"]["rate_EUR"].iloc[0]
        df["rate"] = df["rate_EUR"] / nok_rate_eur
        df_processed = df.drop(columns=["rate_EUR"])
        df_final = df_processed.rename(
            columns={"currency_code": "target_currency_code"}
        )
        return df_final


# --- Related functions that process API data  ---


def _db_upsert_record_currencies_user_method(table: Table, conn, keys, data_iter, **kw):
    """
    Upsert record into currency.currencies table
    """
    pd_dataframe_chunk = pd.DataFrame(list(data_iter), columns=keys)
    values_to_insert = pd_dataframe_chunk.to_dict(orient="records")
    insert_stmt = insert(table.table).values(values_to_insert)

    on_conflict_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["currency_code"],
        set_={"name": insert_stmt.excluded.name, "symbol": insert_stmt.excluded.symbol},
    )
    result = conn.execute(on_conflict_stmt)
    return result.rowcount


def _db_upsert_record_rates_user_method(table: Table, conn, keys, data_iter, **kw):
    """
    Upsert record into currency.currencies table
    """
    pd_dataframe_chunk = pd.DataFrame(list(data_iter), columns=keys)
    values_to_insert = pd_dataframe_chunk.to_dict(orient="records")
    insert_stmt = insert(table.table).values(values_to_insert)

    on_conflict_stmt = insert_stmt.on_conflict_do_update(
        index_elements=["date", "base_currency_code", "target_currency_code"],
        set_={"rate": insert_stmt.excluded.rate,},
    )
    result = conn.execute(on_conflict_stmt)
    return result.rowcount


def upsert_currencies_to_db(currencies_df: pd.DataFrame, db_engine):
    """
    Use SQL Alchemy engine doing bulk insert in DB
    Also uses a custom method for upsert logic
    """

    dtype_mapping = {
        "currency_code": String(3),  # Maps to VARCHAR(3)
        "name": String(100),  # Maps to VARCHAR(100)
        "symbol": String(10),  # Maps to VARCHAR(10)
    }
    with db_engine.connect() as conn:
        with conn.begin():
            rows_affected = currencies_df.to_sql(
                schema="currency",
                name="currencies",
                if_exists="append",
                index=False,
                method=_db_upsert_record_currencies_user_method,
                dtype=dtype_mapping,
                con=conn,
            )
        print("upsertion in to currency.currencies table completed")
        print(f"Total rows affected (inserted/updated): {rows_affected}")


def upsert_rates_to_db(rates_df: pd.DataFrame, db_engine):
    """
    Use SQL Alchemy engine doing bulk insert in DB
    Also uses a custom method for upsert logic
    """

    dtype_mapping = {
        "target_currency_code": String(3),  # Maps to VARCHAR(3)
        "base_currency_code": String(3),
        "rate": Numeric(20, 10),  # Maps to VARCHAR(100)
        "date": Date,  # Maps to VARCHAR(10)
    }

    with db_engine.connect() as conn:
        with conn.begin():
            rows_affected = rates_df.to_sql(
                schema="currency",
                name="currency_conversion_rate",
                if_exists="append",
                index=False,
                method=_db_upsert_record_rates_user_method,
                dtype=dtype_mapping,
                con=conn,
            )
        print("upsertion in to currency.currency_conversion_rate table completed")
        print(f"Total rows affected (inserted/updated): {rows_affected}")
