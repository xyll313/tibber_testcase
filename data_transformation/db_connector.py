import os
import psycopg2
from psycopg2 import Error


class DBConnector:
    """
    A class to manage PostgreSQL database connections and operations.
    """

    def __init__(
        self,
        dbname="aa_41",
        user="myuser",
        password="mypassword",
        host="localhost",
        port="5432",
    ):
        self.dbname = os.getenv("DB_NAME", dbname)
        self.user = os.getenv("DB_USER", user)
        self.password = os.getenv("DB_PASSWORD", password)
        self.host = os.getenv("DB_HOST", host)
        self.port = os.getenv("DB_PORT", port)
        self._connection = None

    def _connect(self):
        """Establishes a new database connection if one is not already open."""
        if self._connection is None or self._connection.closed:
            try:
                print(
                    f"Connecting to PostgreSQL database: {self.dbname} on {self.host}:{self.port}"
                )
                self._connection = psycopg2.connect(
                    dbname=self.dbname,
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                )
                # Set autocommit to True for simpler operations.
                # For complex transactions, you might set this to False and manage commit/rollback manually.
                self._connection.autocommit = True
                print("Connection established successfully.")
            except Error as e:
                print(f"Error connecting to database: {e}")
                self._connection = None  # Ensure connection is None on failure
                raise  # Re-raise the exception to inform the caller
        return self._connection

    def _close(self):
        """Closes the database connection if it's open."""
        if self._connection and not self._connection.closed:
            self._connection.close()
            print("Connection closed.")
        self._connection = None  # Ensure connection is None after closing

    # Context Manager methods for 'with' statement support
    def __enter__(self):
        """Called when entering the 'with' statement. Returns the underlying connection object."""
        return self._connect()

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Called when exiting the 'with' statement. Ensures connection is closed."""
        self._close()

    def _execute_query(self, query: str, params=None):
        """
        Executes a SQL query with optional parameters.
        Manages cursor creation and closing.

        Return:
            fetched data for SELECT queries.
        """
        try:
            with self._connect().cursor() as cursor:
                cursor.execute(query, params)
        except Error as e:
            print(f"Error executing query: {e}")

    # --- Database-specific functions using DBConnector ---

    def init_db(self):
        """
        Re-initiate DB, delete currencies schema and public.item_prices table 
        """
        try:
            self._execute_query("DROP SCHEMA IF EXISTS currency CASCADE")
            self._execute_query("DROP TABLE IF EXISTS public.item_prices CASCADE")
            self._execute_query("DROP View IF EXISTS public.item_prices_NOK CASCADE")
        except Error as e:
            print(f"Error when dropping existing SCHEMA, TABLE, or View: {e}")
        except Exception as e:
            print(
                f"An unexpected error occurred  when dropping existing SCHEMA, TABLE, or View: {e}"
            )

        print("currency Schema and public.item_prices table are dropped if they exist")
        self.create_currencies_table()
        self.create_currency_conversion_rate_table()
        self.create_item_prices_table()

    def create_currencies_table(self):
        """
        Creates the currency.currencies table if it doesn't exist.
        Takes a DBConnector instance to perform the operation.
        """
        POSTGRES_CREATE_CURRENCIES_TABLE_SQL = """
        CREATE SCHEMA IF NOT EXISTS currency;

        CREATE TABLE IF NOT EXISTS currency.currencies (
        currency_code  VARCHAR(3) PRIMARY KEY,
        name           VARCHAR(100) NOT NULL,
        symbol         VARCHAR(10) 
            );
        """
        try:
            self._execute_query(POSTGRES_CREATE_CURRENCIES_TABLE_SQL)
            print(
                "currency.currencies table is created (or already existed) as requested."
            )
        except Error as e:
            print(f"Error creating currency.currencies table: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during currency table creation: {e}")

    def create_currency_conversion_rate_table(self):
        """
        create currency_conversion_rate table

        Return:
            Boolean: indicates wether create table operation is successful or not
        """
        POSTGRES_CREATE_TABLE_SQL = """
        CREATE SCHEMA IF NOT EXISTS currency;

        CREATE TABLE IF NOT EXISTS currency.currency_conversion_rate (
        date                 DATE NOT NULL,
        base_currency_code   VARCHAR(3) NOT NULL,
        target_currency_code VARCHAR(3) NOT NULL,
        rate                 NUMERIC(20,10) NOT NULL,

        PRIMARY KEY (date, base_currency_code,target_currency_code),

        FOREIGN KEY (base_currency_code) REFERENCES currency.currencies(currency_code),
        FOREIGN KEY (target_currency_code) REFERENCES currency.currencies(currency_code)
        );

        """
        try:
            self._execute_query(POSTGRES_CREATE_TABLE_SQL)
            print("currencies.currency_conversion_rate table is created as requested")
        except Error as e:
            print(
                f"Error creating currency.currency_conversion_rate table: {e}"
            )  # Corrected print message
        except Exception as e:
            print(f"An unexpected error occurred during currency table creation: {e}")

    def create_item_prices_table(self):
        """
        Creates the public.item_prices table if it doesn't exist.
        Takes a DBConnector instance to perform the operation.
        """
        POSTGRES_CREATE_TABLE_SQL = """
        CREATE SCHEMA IF NOT EXISTS public;

        CREATE TABLE IF NOT EXISTS public.item_prices (
        id   VARCHAR(100) PRIMARY KEY,
        item  VARCHAR(100),
        price  NUMERIC(5,2),
        currency VARCHAR(3),
        created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ,
        system_timestamp TIMESTAMPTZ,

        FOREIGN KEY (currency) REFERENCES currency.currencies(currency_code)

            );
        """
        try:
            self._execute_query(POSTGRES_CREATE_TABLE_SQL)
            print(
                "public.item_prices table is created (or already existed) as requested."
            )
        except Error as e:
            print(f"Error creating public.item_prices table: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during table creation: {e}")

    def upsert_record_item_prices(
        self, id, item, price, currency, created_at, updated_at, system_timestamp
    ):
        """
        Upserts a record into the public.item_prices table.
        Takes a DBConnector instance to perform the operation.
        """
        if len(currency) != 3:
            print(
                "currency(code) invalid for upsert in item_prices, it should be 3 characters."
            )
            return False

        upsert_sql = """
        INSERT INTO public.item_prices (id, item, price, currency, created_at, updated_at, system_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE
        SET
            item = EXCLUDED.item,
            price = EXCLUDED.price,
            currency = EXCLUDED.currency,
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            system_timestamp = EXCLUDED.system_timestamp;
        """
        params = (id, item, price, currency, created_at, updated_at, system_timestamp)

        try:
            self._execute_query(upsert_sql, params)
        except Error as e:
            print(f"An error occurred during upsert for ID {id}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred during upsert for ID {id}: {e}")

    def create_item_prices_NOK_view(self):
        """
        Create a view converting item_prices converting prices to NOK according to exchange rate
        """
        sql = """
            create view public.item_prices_nok as (
            select p.id, p.item, p.price/cc.rate as price,'NOK' as currency,
            p.created_at, p.updated_at,p.system_timestamp from public.item_prices as p
            left join lateral 
            (
                select c.date, c.rate
                from currency.currency_conversion_rate as c
                where c.date <= date(p.updated_at) and c.target_currency_code = p.currency
                order by c.date desc
                limit 1
            )
            as cc on true)
        """
        try:
            self._execute_query(sql)
            print("public.item_prices_NOK view created successfully ")
        except Error as e:
            print(f"An error when creating VIEW public.item_prices_NOK: {e}")
        except Exception as e:
            print(
                f"An unexpected error occurred during creating VIEW public.item_prices_NOK: {e}"
            )
