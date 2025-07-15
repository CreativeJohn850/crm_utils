import pandas as pd
from matplotlib import pyplot as plt
from sqlalchemy import create_engine, text
import configparser
import logging
from datetime import datetime
from pathlib import Path

# Set BASE_DIR to the directory of the current script
BASE_DIR = Path(__file__).parent.parent

# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def configure_logger(base_dir):
    logger.handlers.clear()  # Clear existing handlers to avoid duplicates
    logger.setLevel(logging.INFO)

    log_dir = Path(base_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / 'update_join_date.log'

    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def update_client_join_date(config_path, ingestion_date):
    """
    Updates the join_date column in the clients table with the earliest estimate date
    for clients with a specific ingestion_date. If no estimates exist, join_date remains NULL.

    Args:
        config_path (str): Path to the config.ini file.
        ingestion_date (str): Date in 'YYYY-MM-DD' format to filter clients by ingested_time.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_USER = config['DATABASE']['DB_USER']
        DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
        DB_HOST = config['DATABASE']['DB_HOST']
        DB_PORT = config['DATABASE']['DB_PORT']
        DB_NAME = config['DATABASE']['DB_NAME']
        BASE_DIR = Path(config['PATHS']['BASE_DIR'])

        # Configure logger
        configure_logger(BASE_DIR)

        # Create database connection
        DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(DB_URL)
        logger.info(f"Connected to database: {DB_NAME}")

        # SQL query to update join_date
        query = text("""
        UPDATE clients c
        SET join_date = (
            SELECT MIN(e.date_issued)
            FROM estimates e
            WHERE e.full_name = c.full_name
        )
        WHERE c.ingested_time::date = :ingestion_date;
        """)

        # Execute the update query with a dictionary for parameters
        with engine.connect() as connection:
            result = connection.execute(query, {"ingestion_date": ingestion_date})
            connection.commit()
            logger.info(f"Updated join_date for clients with ingestion_date {ingestion_date}")
            logger.info(f"Rows affected: {result.rowcount}")

    except Exception as e:
        logger.error(f"Error updating join_date: {str(e)}")
        raise

    finally:
        engine.dispose()
        logger.info("Database connection closed.")


def clients_join_month(config_path):
    """
    Queries the number of clients joined per month based on join_date,
    saves the data as a DataFrame, exports to CSV, and plots it.

    Args:
        config_path (str): Path to the config.ini file.

    Returns:
        pd.DataFrame: DataFrame with query results.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_USER = config['DATABASE']['DB_USER']
        DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
        DB_HOST = config['DATABASE']['DB_HOST']
        DB_PORT = config['DATABASE']['DB_PORT']
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        # Create database connection
        DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(DB_URL)
        logger.info(f"Connected to database: {DB_NAME} for clients_join_month")

        # SQL query to count clients joined per month
        query = text("""
        SELECT 
            TO_CHAR(join_date, 'YYYY-MM') AS month,
            COUNT(*) AS clients_joined
        FROM clients
        WHERE join_date IS NOT NULL
        GROUP BY TO_CHAR(join_date, 'YYYY-MM')
        ORDER BY month;
        """)

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)
            logger.info(f"Retrieved {len(df)} rows from clients_joined_per_month query")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / 'data/clients/clients_per_month.csv'
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved DataFrame to {csv_file}")

        # Plot the data
        plot_clients_joined(df)

        return df

    except Exception as e:
        logger.error(f"Error in clients_join_month: {str(e)}")
        raise

    finally:
        engine.dispose()
        logger.info("Database connection closed.")


def plot_clients_joined(df):
    """
    Plots the number of clients joined per month and saves the plot as a JPG.

    Args:
        df (pd.DataFrame): DataFrame with 'month' and 'clients_joined' columns.
    """
    try:
        # Create plots directory
        plot_dir = BASE_DIR / 'plots'
        plot_dir.mkdir(exist_ok=True)
        plot_file = plot_dir / 'clients_joined_per_month.jpg'

        # Extract years from months
        df['year'] = df['month'].str[:4]
        years = df['year'].unique()

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(12, 6))

        # Plot the data
        ax.plot(df['month'], df['clients_joined'], marker='o', linestyle='-', color='b')

        # Alternate background colors by year
        for i, year in enumerate(years):
            year_months = df[df['year'] == year]['month']
            start_idx = df.index[df['month'] == year_months.iloc[0]].tolist()[0]
            end_idx = df.index[df['month'] == year_months.iloc[-1]].tolist()[0] + 1
            color = 'lightblue' if i % 2 == 0 else 'white'
            ax.add_patch(Rectangle((start_idx - 0.5, ax.get_ylim()[0]),
                                   end_idx - start_idx,
                                   ax.get_ylim()[1] - ax.get_ylim()[0],
                                   facecolor=color, alpha=0.3))

        # Set x-axis ticks to show months and years
        month_labels = [pd.to_datetime(m, format='%Y-%m').strftime('%b') for m in df['month']]
        ax.set_xticks(range(len(df['month'])))
        ax.set_xticklabels(month_labels)

        # Add year labels centered under each year's months
        year_positions = []
        for year in years:
            year_months = df[df['year'] == year]['month']
            mid_idx = df.index[df['month'] == year_months.iloc[len(year_months) // 2]].tolist()[0]
            year_positions.append((mid_idx, year))
        ax2 = ax.twiny()
        ax2.set_xlim(ax.get_xlim())
        ax2.set_xticks([pos for pos, _ in year_positions])
        ax2.set_xticklabels([year for _, year in year_positions])
        ax2.set_xlabel('Year')

        # Double the space between x-axis points
        ax.tick_params(axis='x', pad=10, length=0)

        # Set labels and title
        ax.set_title('Clients Joined Per Month')
        ax.set_xlabel('Month')
        ax.set_ylabel('Number of Clients Joined')
        ax.grid(True)
        plt.tight_layout()

        # Save the plot as JPG
        plt.savefig(plot_file, format='jpg', dpi=300)
        plt.close()
        logger.info(f"Plot saved to {plot_file}")

    except Exception as e:
        logger.error(f"Error plotting clients joined: {str(e)}")
        raise


def update_client_join_date(config_path, ingestion_date):
    """
    Updates the join_date column in the clients table with the earliest estimate date
    for clients with a specific ingestion_date. If no estimates exist, join_date remains NULL.

    Args:
        config_path (str): Path to the config.ini file.
        ingestion_date (str): Date in 'YYYY-MM-DD' format to filter clients by ingested_time.
    """
    try:
        # Read configuration
        config = configparser.ConfigParser()
        config.read(config_path)
        DB_USER = config['DATABASE']['DB_USER']
        DB_PASSWORD = config['DATABASE']['DB_PASSWORD']
        DB_HOST = config['DATABASE']['DB_HOST']
        DB_PORT = config['DATABASE']['DB_PORT']
        DB_NAME = config['DATABASE']['DB_NAME']

        # Configure logger
        configure_logger(BASE_DIR)

        # Create database connection
        DB_URL = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(DB_URL)
        logger.info(f"Connected to database: {DB_NAME}")

        # SQL query to update join_date
        query = text("""
        UPDATE clients c
        SET join_date = (
            SELECT MIN(e.date_issued)
            FROM estimates e
            WHERE e.full_name = c.full_name
        )
        WHERE c.ingested_time::date = :ingestion_date;
        """)

        # Execute the update query
        with engine.connect() as connection:
            result = connection.execute(query, {"ingestion_date": ingestion_date})
            connection.commit()
            logger.info(f"Updated join_date for clients with ingestion_date {ingestion_date}")
            logger.info(f"Rows affected: {result.rowcount}")

    except Exception as e:
        logger.error(f"Error updating join_date: {str(e)}")
        raise

    finally:
        engine.dispose()
        logger.info("Database connection closed.")


if __name__ == "__main__":
    # Default config path and ingestion date
    config_path = 'config.ini'
    ingestion_date = "2025-07-11" # datetime.now().strftime('%Y-%m-%d')

    # call functions
    #update_client_join_date(config_path, ingestion_date)

    # df = clients_join_month(config_path)
    df = pd.read_csv(f"{BASE_DIR}/'data/clients/clients_per_month.csv'")
    plot_clients_joined(df)
    # print(df)
