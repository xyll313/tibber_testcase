import pytest
import os
import psycopg2
from psycopg2 import Error
from unittest.mock import MagicMock, patch
from data_transformation.db_connector import DBConnector

# --- Pytest Fixtures ---
@pytest.fixture
def mock_psycopg2_connect(mocker):
    """Mocks psycopg2.connect and returns the mock object."""
    mock_conn = MagicMock()
    mock_conn.closed = False # Simulate open connection
    mock_conn.cursor.return_value.__enter__.return_value = MagicMock() # Mock cursor context manager
    mocker.patch('psycopg2.connect', return_value=mock_conn)
    return mock_conn

@pytest.fixture
def db_connector_default():
    """Provides a DBConnector instance with default parameters."""
    return DBConnector()

@pytest.fixture
def db_connector_custom():
    """Provides a DBConnector instance with custom parameters."""
    return DBConnector(
        dbname="test_db",
        user="test_user",
        password="test_password",
        host="test_host",
        port="1234",
    )

# Test __init__ method
def test_init_default_params(db_connector_default):
    assert db_connector_default.dbname == "aa_41"
    assert db_connector_default.user == "myuser"
    assert db_connector_default.password == "mypassword"
    assert db_connector_default.host == "localhost"
    assert db_connector_default.port == "5432"
    assert db_connector_default._connection is None

def test_init_env_vars(db_connector_default, monkeypatch):
    monkeypatch.setenv("DB_NAME", "env_db")
    monkeypatch.setenv("DB_USER", "env_user")
    monkeypatch.setenv("DB_PASSWORD", "env_password")
    monkeypatch.setenv("DB_HOST", "env_host")
    monkeypatch.setenv("DB_PORT", "8888")

    connector = DBConnector() # Re-initialize to pick up env vars
    assert connector.dbname == "env_db"
    assert connector.user == "env_user"
    assert connector.password == "env_password"
    assert connector.host == "env_host"
    assert connector.port == "8888"

def test_init_custom_params(db_connector_custom):
    assert db_connector_custom.dbname == "test_db"
    assert db_connector_custom.user == "test_user"
    assert db_connector_custom.password == "test_password"
    assert db_connector_custom.host == "test_host"
    assert db_connector_custom.port == "1234"
    assert db_connector_custom._connection is None

# Test _close method
def test_close_connection_open(db_connector_default, mock_psycopg2_connect):
    db_connector_default._connection = mock_psycopg2_connect # Simulate open connection
    db_connector_default._close()
    mock_psycopg2_connect.close.assert_called_once() # Ensure close() was called on the mock
    assert db_connector_default._connection is None # Ensure internal reference is cleared

def test_close_connection_already_closed(db_connector_default, mock_psycopg2_connect):
    mock_psycopg2_connect.closed = True # Simulate already closed connection
    db_connector_default._connection = mock_psycopg2_connect
    db_connector_default._close()
    mock_psycopg2_connect.close.assert_not_called() # Should not try to close again
    assert db_connector_default._connection is None

def test_close_connection_none(db_connector_default):
    db_connector_default._connection = None # Ensure it's None initially
    db_connector_default._close() # Should do nothing
    assert db_connector_default._connection is None


# Test _execute_query method
def test_execute_query_success(db_connector_default, mock_psycopg2_connect):
    mock_cursor = mock_psycopg2_connect.cursor.return_value.__enter__.return_value # Get the mock cursor
    
    query = "SELECT * FROM users WHERE id = %s;"
    params = (1,)
    
    db_connector_default._execute_query(query, params)
    
    mock_psycopg2_connect.cursor.assert_called_once() # Ensure cursor was obtained
    mock_cursor.execute.assert_called_once_with(query, params) # Ensure execute was called
    # No assert for fetchall as your _execute_query doesn't return it currently.
    # mock_cursor.close() is automatically handled by the 'with' statement for cursor

def test_execute_query_no_params(db_connector_default, mock_psycopg2_connect):
    mock_cursor = mock_psycopg2_connect.cursor.return_value.__enter__.return_value
    query = "CREATE TABLE IF NOT EXISTS my_table (id INT);"
    
    db_connector_default._execute_query(query)
    
    mock_cursor.execute.assert_called_once_with(query, None) # Params should be None

def test_execute_query_connection_error(db_connector_default, mocker, capsys):
    mocker.patch('psycopg2.connect', side_effect=Error("Connection issue"))
    
    db_connector_default._execute_query("SELECT 1;")
    
    captured = capsys.readouterr()
    assert "Error executing query: Connection issue" in captured.out
    psycopg2.connect.assert_called_once() # connect is attempted
    # No cursor.execute call should happen if connect fails
    mock_conn = mocker.MagicMock()
    mock_conn.cursor.assert_not_called()
