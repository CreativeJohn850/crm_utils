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

def clean_string(value):
    if isinstance(value, str):
        return value.strip().replace('"', '')
    return value


def clean_dataframe(df, table_name, engine):
    df = df.apply(lambda x: x.map(clean_string) if x.dtype == "object" else x)
    required_columns = {
        'clients': ['full_name'],
        'dup_name_clients': ['full_name'],
        'estimates': ['estimate_number', 'full_name'],
        'invoices': ['invoice_number', 'full_name']
    }
    for col in required_columns[table_name]:
        if col not in df.columns:
            logger.error(f"Missing required column {col} in {table_name}")
            raise ValueError(f"Missing required column {col}")
    initial_rows = len(df)
    df = df.dropna(subset=required_columns[table_name])
    dropped_rows = initial_rows - len(df)
    if dropped_rows > 0:
        logger.warning(f"Dropped {dropped_rows} rows from {table_name} due to missing required fields")

    if table_name in ['clients', 'dup_name_clients']:
        for col, max_len in [('full_name', 42), ('first_name', 20), ('last_name', 20),
                             ('email_address', 35), ('phone_mobile', 16), ('phone_other', 16),
                             ('address', 50), ('address_2', 50), ('city', 24), ('state_province', 24),
                             ('private_notes', 150)]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: x[:max_len] if isinstance(x, str) and len(x) > max_len else x)
        df['joist_client_id'] = pd.to_numeric(df['joist_client_id'], errors='coerce')
        df['zip_postal_code'] = pd.to_numeric(df['zip_postal_code'], errors='coerce')
        df['ingested_time'] = datetime.now()
        df['source'] = 'joist'

    if table_name in ['estimates', 'invoices']:
        df['estimate_number' if table_name == 'estimates' else 'invoice_number'] = pd.to_numeric(
            df['estimate_number' if table_name == 'estimates' else 'invoice_number'], errors='coerce')
        df['full_name'] = df['full_name'].apply(lambda x: x[:42] if isinstance(x, str) and len(x) > 42 else x)
        df['subtotal'] = pd.to_numeric(df['subtotal'], errors='coerce')
        df['tax'] = pd.to_numeric(df['tax'], errors='coerce')
        df['total'] = pd.to_numeric(df['total'], errors='coerce')
        df['date_issued'] = pd.to_datetime(df['date_issued'], errors='coerce')
        df['date_created'] = pd.to_datetime(df['date_created'], errors='coerce')
        if table_name == 'invoices':
            df['payment_received_less_refunds'] = pd.to_numeric(df['payment_received_less_refunds'], errors='coerce')
        df['ingested_time'] = datetime.now()
        df['source'] = 'joist'
        existing_clients = pd.read_sql("SELECT full_name FROM clients", engine)
        invalid_names = df[~df['full_name'].isin(existing_clients['full_name'])]['full_name'].unique()
        if len(invalid_names) > 0:
            logger.error(f"Invalid full_name values in {table_name}: {invalid_names}")
            raise ValueError(f"Invalid full_name values: {invalid_names}")

    return df

def ingest_clients_1(config_path, date_str=None):
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
        'Email': 'email_address',
        'Mobile': 'phone_mobile',
        'OtherPhone': 'phone_other',
        'Address': 'address',
        'Address2': 'address_2',
        'City': 'city',
        'State': 'state_province',
        'Zip': 'zip_postal_code',
        'Notes': 'private_notes',
        'JoistID': 'joist_client_id'
    }
    
    engine = create_engine(DB_URL)
    file = BASE_DIR / 'data' / 'clients' / f'clients_{date_str}.csv'
    if file.exists():
        df = pd.read_csv(file, dtype=str)
        if 'Name' in df.columns:
            df['Name'] = df['Name'].str.replace(r'[&./\\]', '', regex=True)
            name_splits = df['Name'].str.split(' ', expand=True)
            df['first_name'] = name_splits[0].str[:20]
            df['last_name'] = name_splits[2].str[:20].fillna(name_splits[1].str[:20])
            df = df.rename(columns={'Name': 'full_name'})
        df = df.rename(columns=column_mappings)
        df = clean_dataframe(df, 'clients', engine)
        df.to_sql('clients', engine, if_exists='append', index=False)
        logger.info(f"Ingested {file} into clients")
    else:
        logger.warning(f"No file found: {file}")
    engine.dispose()

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
                # df_unique = clean_dataframe(df_unique, 'clients', engine)
                df_unique.to_sql('clients', engine, if_exists='append', index=False, method='multi')
                logger.info(f"Ingested {len(df_unique)} unique rows into clients")

            # Clean and insert duplicate records into dup_name_clients
            if not df_dup.empty:
                # df_dup = clean_dataframe(df_dup, 'dup_name_clients', engine)
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

def ingest_est_inv(table_name, config_path):
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
            'EstimateNo': 'estimate_number',
            'ClientName': 'full_name',
            'Subtotal': 'subtotal',
            'Tax': 'tax',
            'Total': 'total',
            'IssuedDate': 'date_issued',
            'CreatedDate': 'date_created'
        },
        'invoices': {
            'InvoiceNo': 'invoice_number',
            'ClientName': 'full_name',
            'Subtotal': 'subtotal',
            'Tax': 'tax',
            'Total': 'total',
            'IssuedDate': 'date_issued',
            'CreatedDate': 'date_created',
            'Payments': 'payment_received_less_refunds'
        }
    }
    # print(DB_URL)
    engine = create_engine(DB_URL)
    files = list((BASE_DIR / 'data' / table_name).glob('*.csv'))
    logger.info(f"Found {len(files)} CSV files for {table_name}")
    for file in files:
        df = pd.read_csv(file, dtype=str)
        df = df.rename(columns=column_mappings[table_name])
        df = clean_dataframe(df, table_name, engine)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Ingested {file} into {table_name}")
    engine.dispose()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')
    parser.add_argument('--config', default='config.ini', help='Path to config file')
    parser.add_argument('--date', help='Date for clients file (YYYY_MM_DD)', default=datetime.now().strftime('%Y_%m_%d'))
    args = parser.parse_args()
    # ingest_clients_1(args.config, args.date)
    ingest_clients_2(args.config, args.date)
    # ingest_est_inv("estimates", args.config)
    # ingest_est_inv("invoices", args.config)
    