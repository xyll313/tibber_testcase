import os
import re
import datetime
from datetime import timezone
import pandas as pd


class CsvProcessor:
    def __init__(self, db_connector, related_batch_dir_to_script="../batch_data"):
        self.db_connector = db_connector
        # calculate absolute path for batch dir
        self.batch_data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "../batch_data"
        )
        self.check_point = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        print("csv Processor initialised with batch directory ", self.batch_data_dir)
        print("check point initialised with value : ", self.check_point)

    def _get_batch_number(self, filename: str):
        """
        Reads in filename in format batch1.csv and returns the integer.
        If no number is found then returns inf. 

        Return:
            Integer: batch number
        """
        match = re.search(r"(\d+)\.csv$", filename)
        if match:
            return int(match.group(1))
        return float("inf")

    def _order_csvs(self):
        """
        order csv files based on batch number, ascending.

        Return
            List: orderd csv file names
        """
        csv_file_list = []
        for csv_file in os.listdir(self.batch_data_dir):
            csv_file_list.append(csv_file)

        ordered_csvs = sorted(csv_file_list, key=self._get_batch_number)

        return ordered_csvs

    def process_csvs(self):
        """
        Reads in orderd csvs list and process the files in order.

        Checks updated_at record all date in the field, this can be checked against DB and fetched from API
        Use a check point mechanism to only process csv roles with updated system_timestamp
        Processed data are loaded in price.item_prices table in the DB

        Return:
            List: A list of date from updated_at column
        """
        date_list = []
        ordered_csvs = self._order_csvs()
        for csv in ordered_csvs:
            print(csv, "is processed")
            df = pd.read_csv(self.batch_data_dir + "/" + csv)
            df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
            df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)
            df["system_timestamp"] = pd.to_datetime(df["system_timestamp"], utc=True)
            date_list.extend(df["updated_at"].dt.date.unique())
            for row_tuple in df.itertuples():
                if row_tuple.system_timestamp > self.check_point:
                    try:
                        self.db_connector.upsert_record_item_prices(
                            row_tuple.id,
                            row_tuple.item,
                            row_tuple.price,
                            row_tuple.currency,
                            row_tuple.created_at,
                            row_tuple.updated_at,
                            row_tuple.system_timestamp,
                        )
                        self.check_point = row_tuple.system_timestamp
                    except Exception as e:
                        print(
                            f"An unexpected error occurred when upserting data into : {e}"
                        )

                else:
                    continue
        print("After processing, check point updated to ", self.check_point)
        # remove duplicates
        date_list = sorted(list(set(date_list)))
        return date_list
