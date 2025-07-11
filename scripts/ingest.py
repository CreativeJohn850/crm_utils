import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import configparser
import logging
from datetime import datetime
import argparse
import gc
gc.set_threshold(1000)  # Adjust garbage collection frequency

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_dataframe(df, table_name, engine):
    # Define problematic characters to replace with space
    bad_chars = r'[\0\n\r\t\'"\\%_;]'  # Matches \0, \n, \r, \t, ', ", \, %, _, ;

    # Replace bad characters in string columns only
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        df[col] = df[col].str.replace(bad_chars, ' ', regex=True)

    return df

def ingest_clients_2(config_path, date_str):
    config = configparser.ConfigParser()
    config.read(config_path)
    DB_USER = config['DATABASE']['DB_USER']
    DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
    DB_HOST = config['DATABASE']['DB_HOST']
    DB_PORT = config['DATABASE']['DB_PORT']
    DB_NAME = config['DATABASE']['DB_NAME']
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    BASE_DIR = Path(config['PATHS']['BASE_DIR'])
    
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
    print(DB_URL)
    engine = create_engine(DB_URL)
    file = BASE_DIR / 'data' / 'clients' / f'clients.csv'
    if file.exists():
        try:
            # Read the tab-separated CSV file
            df = pd.read_csv(file, sep='\t', dtype=str, escapechar='\\')
            logger.info(f"Read {file} with {len(df)} rows")

            # Create df_unique (unique full_name, first occurrence) and df_dup (all duplicates)
            df_unique = df.drop_duplicates(subset=['Name'], keep='first')
            df_dup = df[df['Name'].duplicated(keep=False)]

            # Rename columns using column_mappings
            df_unique = df_unique.rename(columns=column_mappings)
            df_dup = df_dup.rename(columns=column_mappings)

            # Set ingested_time from date_str
            ingested_time = pd.to_datetime(date_str, format='%Y_%m_%d')

            # Add ingested_time for both DataFrames
            df_unique['ingested_time'] = ingested_time
            df_dup['ingested_time'] = ingested_time

            # Clean and insert unique records into clients
            if not df_unique.empty:
                df_unique = clean_dataframe(df_unique, 'clients', engine)
                df_unique.to_sql('clients', engine, if_exists='append', index=False, method='multi')
                logger.info(f"Ingested {len(df_unique)} unique rows into clients")

            # Clean and insert duplicate records into dup_name_clients
            if not df_dup.empty:
                df_dup = clean_dataframe(df_dup, 'dup_name_clients', engine)
                df_dup.to_sql('dup_name_clients', engine, if_exists='append', index=False, method='multi')
                logger.info(f"Ingested {len(df_dup)} duplicate rows into dup_name_clients")

        except pd.errors.ParserError as e:
            logger.error(f"CSV parsing error in {file}: {str(e)}")
            with open(file, 'r') as f:
                for i, line in enumerate(f, 1):
                    if i in range(1, 11):  # Log first 10 lines for debugging
                        print(f"Line {i}: {line.strip()}")
            raise
    else:
        logger.warning(f"No file found: {file}")

    engine.dispose()

def ingest_est_inv(table_name, config_path, date_str):
    config = configparser.ConfigParser()
    config.read(config_path)
    DB_USER = config['DATABASE']['DB_USER']
    DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
    DB_HOST = config['DATABASE']['DB_HOST']
    DB_PORT = config['DATABASE']['DB_PORT']
    DB_NAME = config['DATABASE']['DB_NAME']
    DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    BASE_DIR = Path(config['PATHS']['BASE_DIR'])
    
    column_mappings = {
        'estimates': {
            'Estimate #': 'estimate_number',
            'Client Name': 'full_name',
            'Subtotal': 'subtotal',
            'Sales tax': 'tax',     #this is Tax from 2024
            'Total': 'total',
            'Date Issued': 'date_issued',
            'Date Created': 'date_created'
        },
        'invoices': {
            'Invoice #': 'invoice_number',
            'Client Name': 'full_name',
            'Subtotal': 'subtotal',
            'Total': 'total',
            'Tax': 'tax',
            'Date Issued': 'date_issued',
            'Date Created': 'date_created',
            'Payment Received Less Refunds': 'payment_received_less_refunds'
        }
    }
    # print(DB_URL)
    engine = create_engine(DB_URL)
    files = list((BASE_DIR / 'data' / table_name).glob('*.csv'))
    logger.info(f"Found {len(files)} CSV files for {table_name}")
    for file in files:
        print(f"Ingesting file: {file}")
        df = pd.read_csv(file, dtype=str)
        df = df.rename(columns=column_mappings[table_name])
        current_time = datetime.now()

        # Ensure all required columns are present, fill missing with None
        required_columns = {
            'estimates': ['estimate_number', 'full_name', 'subtotal', 'tax', 'total', 'date_issued', 'date_created'],
            'invoices': ['invoice_number', 'full_name', 'subtotal', 'tax', 'total', 'date_issued', 'date_created',
                         'payment_received_less_refunds']
        }
        for col in required_columns[table_name]:
            if col not in df.columns:
                df[col] = None

        # Set ingested_time from date_str
        ingested_time = pd.to_datetime(date_str, format='%Y_%m_%d')

        # Add ingested_time
        df['ingested_time'] = ingested_time

        # Check for missing full_name in clients and insert minimal records
        existing_clients = pd.read_sql("SELECT full_name FROM clients", engine)
        missing_names = df[~df['full_name'].isin(existing_clients['full_name'])]['full_name'].unique()
        if len(missing_names) > 0:
            # Filter out null or empty full_name values and deduplicate
            missing_names = [name for name in missing_names if pd.notnull(name) and name.strip() != '']
            missing_names = pd.Series(missing_names).drop_duplicates().tolist()
            if missing_names:
                logger.info(f"Inserting {len(missing_names)} new clients from invoices")
                new_clients = pd.DataFrame({
                    'full_name': missing_names,
                    'ingested_time': current_time,
                    'source': 'joist'
                })
                new_clients = clean_dataframe(new_clients, table_name, engine)
                new_clients.to_sql('clients', engine, if_exists='append', index=False, method='multi')

        # Drop rows with null or empty full_name to avoid foreign key violation
        initial_rows = len(df)
        df = df[df['full_name'].notnull() & (df['full_name'].str.strip() != '')]
        if len(df) < initial_rows:
            logger.warning(f"Dropped {initial_rows - len(df)} {table_name} rows with null or empty full_name from {file}!")
        df = clean_dataframe(df, table_name, engine)
        df.to_sql(table_name, engine, if_exists='append', index=False, method='multi')
        logger.info(f"Ingested {file} into {table_name}")
    engine.dispose()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')
    parser.add_argument('--config', default='config.ini', help='Path to config file')
    parser.add_argument('--date', help='Date for clients file (YYYY_MM_DD)', default=datetime.now().strftime('%Y_%m_%d'))
    args = parser.parse_args()

    # ingest_clients_2(args.config, args.date)
    ingest_est_inv("estimates", args.config, args.date)
    #ingest_est_inv("invoices", args.config, args.date)
    