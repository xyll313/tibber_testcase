import os
from dotenv import load_dotenv
from db_connector import DBConnector
from api_processor import APIProcessor, upsert_currencies_to_db, upsert_rates_to_db
from csv_reader import CsvProcessor

from sqlalchemy import create_engine


def init():
    """
    Initiate the state, if env.variable REINIT_DB is TRUE then all current tables are dropped 
    And empty tables recreated.

    Returns:
    db_connector: Object used to interact with postgresDB
    """
    db_connector = DBConnector(
        dbname="aa_41",
        user="myuser",
        password="mypassword",
        host="localhost",
        port=os.getenv("DB_PORT"),
    )
    if os.getenv("REINIT_DB") == "TRUE":
        # Task 1.1: create required Tables in DB
        db_connector.init_db()

    return db_connector


def main():
    """
    Running the 3 required tasks
    """

    load_dotenv()
    db_connector = init()
    # Pandas .to_sql currently only accepts SQLAlchemy Engine
    db_engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:"
        f"{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:"
        f"{os.getenv('DB_PORT')}/"
        f"{os.getenv('DB_NAME')}"
    )

    # Task1.2: Populate currency.currencies Table
    api_processor = APIProcessor()
    currencies_df = api_processor.process_currencies()
    upsert_currencies_to_db(currencies_df=currencies_df, db_engine=db_engine)

    # Task2.1: proces csv files
    csv_processor = CsvProcessor(db_connector=db_connector)
    list_of_dates = csv_processor.process_csvs()

    # Task2.2: reads currency conversion rates of the above dates and put them in DB
    for date in list_of_dates:
        rates_df = api_processor.process_hist_rate_NOK(date)
        upsert_rates_to_db(rates_df=rates_df, db_engine=db_engine)

    # Task3 create SQL view
    db_connector.create_item_prices_NOK_view()

    db_connector._close()


if __name__ == "__main__":
    main()
