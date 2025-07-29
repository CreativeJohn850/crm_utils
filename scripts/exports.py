import configparser
import logging
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

# Set BASE_DIR to the directory of the current script
BASE_DIR = Path(__file__).parent.parent

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def configure_logger(base_dir):
    """Configure logger with file and stream handlers."""
    logger.handlers.clear()  # Clear existing handlers to avoid duplicates
    logger.setLevel(logging.INFO)

    log_dir = Path(base_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'export_data.log'

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def export_customers(config_path, engine):
    """
    Exports full_name and email_address of clients who have invoices and valid email addresses to customers.csv.

    Args:
        config_path (str): Path to the config.ini file.
        engine: SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        logger.info(f"Using database: {DB_NAME} for export_customers")

        # SQL query to select clients with invoices and valid email_address
        query = text("""
        SELECT DISTINCT c.full_name, c.email_address
        FROM clients c
        JOIN invoices i ON c.full_name = i.full_name
        WHERE c.email_address IS NOT NULL
        AND NOT (
            c.email_address LIKE '%@%@%'  -- Multiple @ symbols
            OR c.email_address LIKE '%,%'  -- Contains commas
            OR c.email_address LIKE '% %'  -- Contains spaces
            OR c.email_address ~ '[^a-zA-Z0-9.@_-]'  -- Contains invalid characters
        )
        ORDER BY c.full_name;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows for customers with invoices and valid emails")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/customers.csv'
        csv_file.parent.mkdir(exist_ok=True)
        df = df.drop_duplicates(subset='email_address', keep='first')
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved customers data to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error in export_customers: {str(e)}")
        raise

def export_leads(config_path, engine):
    """
    Exports full_name and email_address of clients who have estimates but no invoices and valid email addresses to leads.csv.

    Args:
        config_path (str): Path to the config.ini file.
        engine: SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        logger.info(f"Using database: {DB_NAME} for export_leads")

        # SQL query to select clients with estimates but no invoices and valid email_address
        query = text("""
        SELECT DISTINCT c.full_name, c.email_address
        FROM clients c
        JOIN estimates e ON c.full_name = e.full_name
        LEFT JOIN invoices i ON c.full_name = i.full_name
        WHERE i.full_name IS NULL 
        AND c.email_address IS NOT NULL
        AND NOT (
            c.email_address LIKE '%@%@%'  -- Multiple @ symbols
            OR c.email_address LIKE '%,%'  -- Contains commas
            OR c.email_address LIKE '% %'  -- Contains spaces
            OR c.email_address ~ '[^a-zA-Z0-9.@_-]'  -- Contains invalid characters
        )
        ORDER BY c.full_name;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows for leads with estimates but no invoices and valid emails")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/leads.csv'
        csv_file.parent.mkdir(exist_ok=True)
        df = df.drop_duplicates(subset='email_address', keep='first')
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved leads data to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error in export_leads: {str(e)}")
        raise

def export_all_clients(config_path, engine):
    """
    Exports full_name and email_address of all unique clients with valid email addresses to all_clients.csv.

    Args:
        config_path (str): Path to the config.ini file.
        engine: SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        logger.info(f"Using database: {DB_NAME} for export_all_clients")

        # SQL query to select all unique clients with valid email_address
        query = text("""
        SELECT DISTINCT full_name, email_address
        FROM clients
        WHERE email_address IS NOT NULL
        AND NOT (
            email_address LIKE '%@%@%'  -- Multiple @ symbols
            OR email_address LIKE '%,%'  -- Contains commas
            OR email_address LIKE '% %'  -- Contains spaces
            OR email_address ~ '[^a-zA-Z0-9.@_-]'  -- Contains invalid characters
        )
        ORDER BY full_name;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows for all unique clients with valid emails")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/all_clients.csv'
        csv_file.parent.mkdir(exist_ok=True)
        df = df.drop_duplicates(subset='email_address', keep='first')
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved all clients data to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error in export_all_clients: {str(e)}")
        raise


def export_clients_without_email(config_path, engine):
    """
    Exports all columns of clients with NULL email_address to clients_without_email.csv.

    Args:
        config_path (str): Path to the config.ini file.
        engine: SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        logger.info(f"Using database: {DB_NAME} for export_clients_without_email")

        # SQL query to select all columns for clients with NULL email_address
        query = text("""
        SELECT *
        FROM clients
        WHERE email_address IS NULL
        ORDER BY full_name;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows for clients with no email address")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/clients_without_email.csv'
        csv_file.parent.mkdir(exist_ok=True)
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved clients without email data to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error in export_clients_without_email: {str(e)}")
        raise

def email_issues(config_path, engine):
    """
    Exports all columns of clients with invalid email addresses (commas, spaces, or invalid characters) to email_issues.csv.

    Args:
        config_path (str): Path to the config.ini file.
        engine: SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        logger.info(f"Using database: {DB_NAME} for email_issues")

        # SQL query to select all columns for clients with invalid email addresses
        query = text("""
        SELECT *
        FROM clients
        WHERE email_address IS NOT NULL
        AND (
            email_address LIKE '%,%'  -- Contains commas
            OR email_address LIKE '% %'  -- Contains spaces
            OR email_address ~ '[^a-zA-Z0-9.@_-]'  -- Contains invalid characters
        )
        ORDER BY full_name;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows for clients with email issues")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/email_issues.csv'
        csv_file.parent.mkdir(exist_ok=True)
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved clients with email issues data to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error in email_issues: {str(e)}")
        raise

def more_emails(config_path, engine):
    """
    Exports all columns of clients with email addresses containing multiple @ symbols to multiple_emails.csv.

    Args:
        config_path (str): Path to the config.ini file.
        engine: SQLAlchemy engine for database connection.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        logger.info(f"Using database: {DB_NAME} for more_emails")

        # SQL query to select all columns for clients with multiple @ symbols in email
        query = text("""
        SELECT *
        FROM clients
        WHERE email_address IS NOT NULL
        AND email_address LIKE '%@%@%'
        ORDER BY full_name;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows for clients with multiple @ symbols in email")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/multiple_emails.csv'
        csv_file.parent.mkdir(exist_ok=True)
        df = df.drop_duplicates(subset='email_address', keep='first')
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved clients with multiple @ emails data to {csv_file}")

        return df

    except Exception as e:
        logger.error(f"Error in more_emails: {str(e)}")
        raise


if __name__ == "__main__":
    # Default config path
    config_path = 'config.ini'

    # Read configuration
    config = configparser.ConfigParser()
    config.read(config_path)
    DB_USER = config['DATABASE']['DB_USER']
    DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
    DB_HOST = config['DATABASE']['DB_HOST']
    DB_PORT = config['DATABASE']['DB_PORT']
    DB_NAME = config['DATABASE']['DB_NAME']

    # Create single database connection
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(DB_URL)
    logger.info(f"Connected to database: {DB_NAME}")

    try:
        # Export customers with invoices
        df_customers = export_customers(config_path, engine)
        # Export leads with estimates but no invoices
        df_leads = export_leads(config_path, engine)
        # Export all unique clients
        df_all_clients = export_all_clients(config_path, engine)
        # Export clients with no email address
        # df_clients_without_email = export_clients_without_email(config_path, engine)
        # #Export clients with problematic email addresses
        # email_issues(config_path, engine)
        # # Export clients with multiple email addresses in the field
        # more_emails(config_path, engine)

    except Exception as e:
        logger.error(f"Error during export operations: {str(e)}")
        raise

    finally:
        engine.dispose()
        logger.info("Database connection closed.")