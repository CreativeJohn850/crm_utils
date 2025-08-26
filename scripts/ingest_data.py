import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from datetime import datetime

# Cleans problematic characters for the database
# Args: df = dataframe to clean
#       cols = string columns hand-picked for each df

def clean_df_cols(df, cols):
    # Define problematic characters to replace with space
    bad_chars = r'[\0\n\r\t\'"\\%_;]'  # Matches \0, \n, \r, \t, ', ", \, %, _, ;

    # Replace bad characters in cols only
    for col in cols:
        df[col] = df[col].str.replace(bad_chars, ' ', regex=True).str.strip()

    return df

def ingest_estimate_invoice(df, table_name, config_path, date, logger):
    """
    Shared helper method to handle the core data ingestion logic,
    now accepting a DataFrame and logger directly.
    """
    config = configparser.ConfigParser()
    if not config.read(config_path):
        logger.error(f"Config file not found at {config_path}")
        raise FileNotFoundError(f"Config file not found at {config_path}")
    DB_USER = config['DATABASE']['DB_USER']
    DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
    DB_HOST = config['DATABASE']['DB_HOST']
    DB_PORT = config['DATABASE']['DB_PORT']
    DB_NAME = config['DATABASE']['DB_NAME']
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    column_mappings = {
        'estimates': {
            'Estimate #': 'estimate_number', 'Client Name': 'full_name', 'Subtotal': 'subtotal',
            'Tax': 'tax', 'Total': 'total', 'Date Issued': 'date_issued',
            'Date Created': 'date_created'
        },
        'invoices': {
            'Invoice #': 'invoice_number', 'Client Name': 'full_name', 'Subtotal': 'subtotal',
            'Total': 'total', 'Tax': 'tax', 'Date Issued': 'date_issued',
            'Date Created': 'date_created', 'Payment Received Less Refunds': 'payment_received_less_refunds'
        }
    }

    engine = create_engine(DB_URL)
    logger.info(f"Connected to database: {DB_URL.replace(DB_PASSWORD, '****')}")

    print(column_mappings[table_name])
    # Rename columns based on the table name
    df.rename(columns=column_mappings[table_name], inplace=True)
    print(df.info())
    logger.debug(f"Renamed columns in {table_name} DataFrame: {list(df.columns)}")

    # Ensure all required columns are present, fill missing with None
    required_columns = {
        'estimates': ['estimate_number', 'full_name', 'subtotal', 'tax', 'total', 'date_issued', 'date_created'],
        'invoices': ['invoice_number', 'full_name', 'subtotal', 'tax', 'total', 'date_issued', 'date_created',
                     'payment_received_less_refunds']
    }
    for col in required_columns[table_name]:
        if col not in df.columns:
            df[col] = None
            logger.warning(f"Missing column {col} in {table_name} DataFrame, filled with None")

    df['ingested_date'] = pd.to_datetime(date).date()
    logger.debug(f"Assigned ingested_date: {df['ingested_date'].iloc[0]} for {len(df)} {table_name} records")

    try:
        cols = ['full_name']
        df = clean_df_cols(df, cols)
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        logger.info(f"Ingested {len(df)} rows into {table_name}")
    except Exception as e:
        logger.error(f"Failed to write data to {table_name}: {str(e)}")
        raise
    finally:
        engine.dispose()

def ingest_clients_with_estimates(estimates, clients, config_path, ingestion_date, logger):
    # Load configuration
    config = configparser.ConfigParser()
    if not config.read(config_path):
        logger.error(f"Config file not found at {config_path}")
        raise FileNotFoundError(f"Config file not found at {config_path}")
    DB_USER = config['DATABASE']['DB_USER']
    DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
    DB_HOST = config['DATABASE']['DB_HOST']
    DB_PORT = config['DATABASE']['DB_PORT']
    DB_NAME = config['DATABASE']['DB_NAME']
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

    # Rename columns in clients to match PostgreSQL schema
    column_mappings = {
        'Name': 'full_name',
        'Email Address': 'email_address',
        'Phone (mobile)': 'phone_mobile',
        'Phone (other)': 'phone_other',
        'Address': 'address',
        'Address 2': 'address_2',
        'City': 'city',
        'State / Province': 'state_province',
        'Zip / Postal Code': 'zip_postal_code',
        'Private Notes': 'private_notes',
        '**(Do not change this) Joist Client ID': 'joist_client_id'
    }
    clients = clients.rename(columns=column_mappings)
    # print(clients.info())
    logger.debug(f"Renamed columns in clients DataFrame: {list(clients.columns)}")

    estimate_column_mappings = {
        'Estimate #': 'estimate_number', 'Client Name': 'full_name', 'Subtotal': 'subtotal',
        'Tax': 'tax', 'Total': 'total', 'Date Issued': 'date_issued',
        'Date Created': 'date_created'
    }
    estimates = estimates.rename(columns=estimate_column_mappings)

    # Populate join_date with the earliest date_created from estimates
    # print(estimates.info())
    join_dates = estimates.groupby('full_name')['date_created'].min().reset_index()
    # print(type(join_dates))
    print("***"*25)
    # print(join_dates.info())
    clients = clients.merge(join_dates, on='full_name', how='left')
    clients['join_date'] = pd.to_datetime(clients['date_created']).dt.date
    clients = clients.drop(columns=['date_created'], errors='ignore')
    logger.info(f"Populated join_date for {len(clients[~clients['join_date'].isna()])} clients")

    # Create engine
    try:
        engine = create_engine(DB_URL)
        logger.info(f"Connected to database: {DB_URL.replace(DB_PASSWORD, '****')}")
        # Load existing clients from database
        query = text("SELECT * FROM clients;")
        with engine.connect() as connection:
            db_clients = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(db_clients)} rows for clients")

            # Create sets for fast lookups
            db_names = set(db_clients['full_name'])
            client_names = set(clients['full_name'])

            # Find new full_names from estimates not in db_clients
            new_names = set(estimates['full_name']) - db_names
            logger.info(f"New clients: {len(new_names)}")

            # Check for case mismatches
            mismatched_names = [name for name in new_names if name.lower() in [n.lower() for n in db_names.union(client_names)]]
            if mismatched_names:
                logger.warning(f"Potential case mismatch for full_name: {mismatched_names}")

            # Identify orphaned estimates
            orphaned_names = new_names - client_names
            logger.info(f"Estimates with orphaned clients: {len(orphaned_names)}")
            if orphaned_names:
                orphaned_df = estimates[estimates['full_name'].isin(orphaned_names)]
                for _, row in orphaned_df.iterrows():
                    logger.info(f"Estimate with {row['estimate_number']} orphaned")

            # Identify full_names to insert
            insert_names = new_names & client_names
            if insert_names:
                # Select rows for insertion
                to_insert = clients[clients['full_name'].isin(insert_names)].copy()
                # Add ingested_date using ingestion_date parameter
                try:
                    to_insert['ingested_date'] = pd.to_datetime(ingestion_date).date()
                    logger.debug(f"Assigned ingested_date: {to_insert['ingested_date'].iloc[0]} for {len(to_insert)} clients")
                except ValueError as e:
                    logger.error(f"Invalid ingestion_date format: {str(e)}")
                    raise
                logger.debug(f"Inserting clients with full_names: {', '.join(to_insert['full_name'])}")
                cols = ['full_name', 'email_address', 'address', 'address_2', 'city', 'state_province', 'private_notes']
                to_insert = clean_df_cols(to_insert, cols)
                try:
                    to_insert.to_sql('clients', engine, if_exists='append', index=False)
                    logger.info(f"Inserted {len(to_insert)} new clients")
                except Exception as e:
                    logger.error(f"Failed to insert clients: {str(e)}")
                    raise
        engine.dispose()

    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        raise
