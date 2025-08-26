"""
    **************************** NOTICE ***********************************
    ******    A lead can have multiple Estimates                     ******
    ******    Any modifications generate a new Estimate              ******
    ******    An Estimate created is a hot lead                      ******
    ******    The Estimate entries connect to clients-mo.csv         ******
    ******    to fill up the columns in clients table                ******
    ***********************************************************************

        This script
        1. reads and ingests into SQL all the ESTIMATES export of a certain month
        2. reads and ingests into SQL all the new CLIENTS
            2.1 reads all clients of all time export 
            2.2 reads all clients of SQL database
            2.3 for all full_name in estimates_df but not in SQL clients,
            uses all time export data to insert them into clients
            all time export contains duplicates for the Name
                - if there are multiple rows with the same Name, we take the one with the highest Joist Client ID
        3. reads and ingests into SQL all the INVOICES export of a certain month
"""
import pandas as pd
from ingest_data import *
from datetime import datetime
import logging
from pathlib import Path
import configparser
import time

def configure_logger(name, base_dir):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    handler = logging.FileHandler(base_dir / f"{name}.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

if __name__ == '__main__':
    c_month = '7'  # current ingestion month
    config_file_path = 'config.ini'
    ingestion_date_str = '2025-08-26'
    ingestion_date = datetime.strptime(ingestion_date_str, '%Y-%m-%d').date()

    # Load configuration to get BASE_DIR
    config = configparser.ConfigParser()
    if not config.read(config_file_path):
        raise FileNotFoundError(f"Config file not found at {config_file_path}")
    BASE_DIR = Path(config['PATHS']['BASE_DIR'])

    # Configure loggers
    estimates_logger = configure_logger('estimates', BASE_DIR)
    clients_logger = configure_logger('clients', BASE_DIR)
    invoices_logger = configure_logger('invoices', BASE_DIR)

    clients_logger.info(f"Starting ingestion process for month {c_month}")

    # Read estimates data
    estimates = pd.read_csv(f'../data/estimates/2025/2025-{c_month}.csv')
    clients_logger.info(f"Read {len(estimates)} estimates from ../data/estimates/2025/2025-{c_month}.csv")

    # Read clients data and remove duplicates
    clients = pd.read_csv(f'../data/clients/2025/Clients.csv')
    clients_logger.info(f"Read {len(clients)} clients from ../data/clients/2025/Clients.csv")
    clients = clients.sort_values('**(Do not change this) Joist Client ID', ascending=False).drop_duplicates(
        subset=['Name'], keep='first')
    clients_logger.info(f"After deduplication, {len(clients)} unique clients remain")

    # Ingest clients with timing - before ingesting estimates ( foreign key constraint )
    start_time = time.time()
    # ingest_clients_with_estimates(estimates, clients, config_file_path, ingestion_date, logger=clients_logger)
    elapsed_time = time.time() - start_time
    clients_logger.info(f"Completed ingest_clients_with_estimates in {elapsed_time:.2f} seconds")

    # Ingest estimates with timing
    start_time = time.time()
    # ingest_estimate_invoice(estimates, table_name='estimates', config_path=config_file_path, date=ingestion_date, logger=estimates_logger)
    elapsed_time = time.time() - start_time
    clients_logger.info(f"Completed ingest_estimate_invoice for estimates in {elapsed_time:.2f} seconds")


    # Read invoices data
    invoices = pd.read_csv(f'../data/invoices/2025/2025-{c_month}.csv', sep='\t')
    clients_logger.info(f"Read {len(invoices)} invoices from ../data/invoices/2025/2025-{c_month}.csv")

    # Ingest invoices with timing
    start_time = time.time()
    ingest_estimate_invoice(invoices, table_name='invoices', config_path=config_file_path, date=ingestion_date, logger=invoices_logger)
    elapsed_time = time.time() - start_time
    clients_logger.info(f"Completed ingest_estimate_invoice for invoices in {elapsed_time:.2f} seconds")

    clients_logger.info(f"Finished ingestion process for month {c_month}")