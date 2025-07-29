import configparser
import logging
from datetime import datetime
from pathlib import Path
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Rectangle
from sqlalchemy import create_engine, text

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

# db query functions --> update
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

# db query functions --> selects
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

        return df

    except Exception as e:
        logger.error(f"Error in clients_join_month: {str(e)}")
        raise

    finally:
        engine.dispose()
        logger.info("Database connection closed.")

def stats_month(config_path, sql_query, table, csv_filename):
    """
    Queries data based on a SQL statement, saves it as a DataFrame, exports to CSV, and plots it.

    Args:
        config_path (str): Path to the config.ini file.
        sql_query (str): SQL statement to execute.
        table (str): Name of the table being queried.
        csv_filename (str): Path to save the CSV output relative to BASE_DIR.

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
        logger.info(f"Connected to database: {DB_NAME} for {table}")

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(text(sql_query), connection)
            logger.info(f"Retrieved {len(df)} rows from {table} ")

        # Save DataFrame to CSV
        csv_file = BASE_DIR / csv_filename
        df.to_csv(csv_file, index=False)
        logger.info(f"Saved DataFrame to {csv_file}")
        return df

    except Exception as e:
        logger.error(f"Error in {table}: {str(e)}")
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

        # Generate filename with current day, hour, and minute (CDT, July 15, 2025, 11:38 AM)
        current_time = datetime(2025, 7, 15, 11, 38)  # Fixed to system-provided time
        time_str = current_time.strftime('%Y-%m-%d_%H-%M')
        plot_file = plot_dir / f'clients_joined_per_month_{time_str}.jpg'

        # Extract years from months
        df['year'] = df['month'].str[:4]
        years = df['year'].unique()

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(34, 6))

        # Plot the data
        ax.plot(df['month'], df['clients_joined'], marker='o', linestyle='-', color='b')

        # Alternate background colors by year (January to December)
        for i, year in enumerate(years):
            # Filter months for the current year
            year_months = df[df['year'] == year]['month']
            if not year_months.empty:
                # Find the first and last month indices for this year
                start_month = year + '-01'  # January
                end_month = year + '-12'  # December
                start_idx = df.index[df['month'] == start_month].tolist()
                end_idx = df.index[df['month'] == end_month].tolist()
                start_idx = start_idx[0] if start_idx else 0
                end_idx = end_idx[0] if end_idx else len(df) - 1

                # Adjust end_idx to ensure it doesn't exceed data range
                end_idx = min(end_idx, len(df) - 1) if end_idx != -1 else len(df) - 1
                if start_idx <= end_idx:
                    color = 'lightblue' if i % 2 == 0 else 'white'
                    ax.add_patch(Rectangle((start_idx - 0.5, ax.get_ylim()[0]),
                                           end_idx - start_idx + 1,
                                           ax.get_ylim()[1] - ax.get_ylim()[0],
                                           facecolor=color, alpha=0.3))

        # Set x-axis ticks with increased spacing (every 2 months)
        num_months = len(df['month'])
        tick_interval = 1
        ax.set_xticks(range(0, num_months, tick_interval))
        month_labels = [pd.to_datetime(m, format='%Y-%m').strftime('%b') for m in df['month']]
        ax.set_xticklabels(
            [month_labels[i] if i < len(month_labels) else '' for i in range(0, num_months, tick_interval)],
            rotation=45, ha='right')

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
        ax.tick_params(axis='x', pad=30, length=0)

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

def plot_years(years, df):
    """
    Plots the number of clients joined per month for specified years and saves the plot as a JPG.

    Args:
        years (list): List of years (e.g., ['2024', '2025']) to plot.
        df (pd.DataFrame): DataFrame with 'month' and 'clients_joined' columns.
    """
    try:

        # Create plots directory
        plot_dir = BASE_DIR / 'plots'
        plot_dir.mkdir(exist_ok=True)

        # Generate filename with current day, hour, and minute (CDT, July 15, 2025, 11:46 AM)
        current_time = datetime(2025, 7, 15, 11, 46)  # Fixed to system-provided time
        time_str = current_time.strftime('%Y-%m-%d_%H-%M')
        plot_file = plot_dir / f'multiple_years_{time_str}.jpg'

        # Define 7 distinct colors
        colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(15, 6))

        # Prepare data for each year
        for i, year in enumerate(years):
            year_data = df[df['month'].str.startswith(year)]
            if not year_data.empty:
                # Create a complete month series with 12 values (Jan to Dec)
                monthly_data = {month: 0 for month in month_order}
                for month in year_data['month']:
                    month_num = pd.to_datetime(month, format='%Y-%m').month - 1  # 0-based index
                    monthly_data[month_order[month_num]] = \
                    year_data[year_data['month'] == month]['clients_joined'].iloc[0]
                ax.plot(month_order, list(monthly_data.values()),
                        marker='o', linestyle='-', color=colors[i % len(colors)],
                        label=year, markersize=4)  # Reduced marker size from default 6 to 4

        # Set labels and title
        ax.set_title('Clients Joined Per Month by Year')
        ax.set_xlabel('Month')
        ax.set_ylabel('Number of Clients Joined')
        ax.grid(True)
        ax.legend(title='Years')

        # Set x-axis ticks for all 12 months
        ax.set_xticks(range(12))
        ax.set_xticklabels(month_order)

        plt.tight_layout()

        # Save the plot as JPG
        plt.savefig(plot_file, format='jpg', dpi=300)
        plt.close()
        logger.info(f"Plot saved to {plot_file}")

    except Exception as e:
        logger.error(f"Error plotting years: {str(e)}")
        raise

def plot_years_wp_old(df, title, xlabel, ylabel, filename_prefix):
    """
    Plots data per month for specified years and saves the plot as a JPG.

    Args:
        df (pd.DataFrame): DataFrame with 'month' and a value column (e.g., 'clients_joined', 'estimates_count').
        title (str): Title of the plot.
        xlabel (str): Label for the x-axis.
        ylabel (str): Label for the y-axis.
        filename_prefix (str): Prefix for the JPG filename (e.g., 'multiple_clients', 'multiple_estimates').
    """
    try:

        # Create plots directory
        plot_dir = BASE_DIR / 'plots'
        plot_dir.mkdir(exist_ok=True)

        # Generate filename with current day, hour, and minute (CDT, July 15, 2025, 12:25 PM)
        current_time = datetime(2025, 7, 15, 12, 25)  # Fixed to system-provided time
        time_str = current_time.strftime('%Y-%m-%d_%H-%M')
        plot_file = plot_dir / f'{filename_prefix}_{time_str}.jpg'

        # Define 7 distinct colors
        colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        # Extract unique years from the month column
        df['year'] = df['month'].str[:4]
        years = df['year'].unique()

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(15, 6))

        # Prepare data for each year
        value_column = df.columns[1]  # Assumes second column is the value (e.g., 'clients_joined')
        for i, year in enumerate(years):
            year_data = df[df['month'].str.startswith(year)]
            if not year_data.empty:
                # Create a complete month series with 12 values (Jan to Dec)
                monthly_data = {month: 0 for month in month_order}
                for month in year_data['month']:
                    month_num = pd.to_datetime(month, format='%Y-%m').month - 1  # 0-based index
                    monthly_data[month_order[month_num]] = year_data[year_data['month'] == month][value_column].iloc[0]
                ax.plot(month_order, list(monthly_data.values()),
                        marker='o', linestyle='-', color=colors[i % len(colors)],
                        label=year, markersize=4)  # Reduced marker size from default 6 to 4

        # Set labels and title
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(True)
        ax.legend(title='Years')

        # Set x-axis ticks for all 12 months
        ax.set_xticks(range(12))
        ax.set_xticklabels(month_order)

        plt.tight_layout()

        # Save the plot as JPG
        plt.savefig(plot_file, format='jpg', dpi=300)
        plt.close()
        logger.info(f"Plot saved to {plot_file}")

    except Exception as e:
        logger.error(f"Error plotting years: {str(e)}")
        raise

def plot_years_wp(df, years, title, xlabel, ylabel, filename_prefix):
    """
    Plots data per month for specified years and saves the plot as a JPG.

    Args:
        df (pd.DataFrame): DataFrame with 'month' and a value column (e.g., 'clients_joined', 'total_invoices').
        years (list): List of years (e.g., ['2024', '2025']) to plot.
        title (str): Title of the plot.
        xlabel (str): Label for the x-axis.
        ylabel (str): Label for the y-axis.
        filename_prefix (str): Prefix for the JPG filename (e.g., 'multiple_clients', 'multiple_estimates').
    """
    try:

        # Create plots directory
        plot_dir = BASE_DIR / 'plots'
        plot_dir.mkdir(exist_ok=True)

        # Generate filename with current day, hour, and minute (CDT, July 15, 2025, 12:44 PM)
        current_time = datetime(2025, 7, 15, 12, 44)  # Fixed to system-provided time
        time_str = current_time.strftime('%Y-%m-%d_%H-%M')
        plot_file = plot_dir / f'{filename_prefix}_{time_str}.jpg'

        # Define 7 distinct colors
        colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k']
        month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

        # Create figure and axis
        fig, ax = plt.subplots(figsize=(15, 6))

        # Prepare data for each specified year
        value_column = df.columns[1]  # Assumes second column is the value (e.g., 'clients_joined', 'total_invoices')
        current_year = datetime.now().year  # 2025
        for i, year in enumerate(years):
            year_data = df[df['month'].str.startswith(str(year))]
            if not year_data.empty:
                # Create a complete month series with 12 values (Jan to Dec)
                monthly_data = {month: 0 for month in month_order}
                for month in year_data['month']:
                    month_num = pd.to_datetime(month, format='%Y-%m').month - 1  # 0-based index
                    monthly_data[month_order[month_num]] = year_data[year_data['month'] == month][value_column].iloc[0]
                # For the current year (2025), do not paint zeros for July to December
                if int(year) == current_year:
                    last_month = 5  # June is month 6, 0-based index is 5
                    for m in range(last_month + 1, 12):
                        monthly_data[month_order[m]] = None
                ax.plot(month_order, [v if v is not None else float('nan') for v in list(monthly_data.values())],
                        marker='o', linestyle='-', color=colors[i % len(colors)],
                        label=year, markersize=4)  # Reduced marker size from default 6 to 4

        # Set labels and title
        ax.set_title(title)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(True)
        ax.legend(title='Years')

        # Set x-axis ticks for all 12 months
        ax.set_xticks(range(12))
        ax.set_xticklabels(month_order)

        plt.tight_layout()

        # Save the plot as JPG
        plt.savefig(plot_file, format='jpg', dpi=300)
        plt.close()
        logger.info(f"Plot saved to {plot_file}")

    except Exception as e:
        logger.error(f"Error plotting years: {str(e)}")
        raise


if __name__ == "__main__":
    # Default config path and ingestion date
    config_path = 'config.ini'
    ingestion_date = "2025-07-11" # datetime.now().strftime('%Y-%m-%d')

    # call functions
    #update_client_join_date(config_path, ingestion_date)

    # df = clients_join_month(config_path)
    # df = pd.read_csv(BASE_DIR / 'data' / 'clients' / 'clients_per_month.csv')
    # plot_clients_joined(df)
    years= ['2019', '2020', '2021', '2024', '2025']
    # plot_years(years, df)
    # print(df)


    # Example 1: Number of new clients (current version)
    clients_query = """
    SELECT 
        TO_CHAR(join_date, 'YYYY-MM') AS month,
        COUNT(*) AS clients_joined
    FROM clients
    WHERE join_date IS NOT NULL
    GROUP BY TO_CHAR(join_date, 'YYYY-MM')
    ORDER BY month;
    """
    df_clients = stats_month(config_path, clients_query, 'clients', 'clients_per_month.csv')
    # print("Number of new clients per month:")
    # print(df_clients)
    plot_years_wp(df_clients, years, 'Clients Joined Per Month by Year', 'Month', 'Number of Clients Joined', 'multiple_clients')

 # Example 2: Number of estimates per month
    estimates_query = """
    SELECT 
        TO_CHAR(date_issued, 'YYYY-MM') AS month,
        COUNT(*) AS estimates_count
    FROM estimates
    GROUP BY TO_CHAR(date_issued, 'YYYY-MM')
    ORDER BY month;
    """
    df_estimates = stats_month(config_path, estimates_query, 'estimates', 'estimates_per_month.csv')
    # print("Number of estimates per month:")
    # print(df_estimates)
    plot_years_wp(df_estimates, years, 'Estimates Per Month by Year', 'Month', 'Number of Estimates', 'multiple_estimates')

    # Example 3: Number of invoices per month
    invoices_query = """
    SELECT 
        TO_CHAR(date_issued, 'YYYY-MM') AS month,
        COUNT(*) AS invoices_count
    FROM invoices
    GROUP BY TO_CHAR(date_issued, 'YYYY-MM')
    ORDER BY month;
    """
    df_invoices = stats_month(config_path, invoices_query, 'invoices', 'invoices_per_month.csv')
    # print("Number of invoices per month:")
    # print(df_invoices)
    plot_years_wp(df_invoices, years, 'Invoices Per Month by Year', 'Month', 'Number of Invoices', 'multiple_invoices')

    # Example 4: Sum of invoices per month
    sum_invoices_query = """
    SELECT 
        TO_CHAR(i.date_issued, 'YYYY-MM') AS month,
        SUM(i.total) AS total_invoices
    FROM invoices i
    GROUP BY TO_CHAR(i.date_issued, 'YYYY-MM')
    ORDER BY month;
    """
    df_sum_invoices = stats_month(config_path, sum_invoices_query, 'invoices', 'sum_invoices_per_month.csv')
    # print("Sum of invoices per month:")
    # print(df_sum_invoices)
    plot_years_wp(df_sum_invoices, years,'Total Invoice Value Per Month by Year', 'Month', 'Total Invoice Value', 'multiple_invoice_values')

    # Example 5: Top 5 customers with most invoice value total per month
    top5_high_query = """
    SELECT 
        TO_CHAR(i.date_issued, 'YYYY-MM') AS month,
        c.full_name,
        SUM(i.total) AS total_value
    FROM invoices i
    JOIN clients c ON i.full_name = c.full_name
    GROUP BY TO_CHAR(i.date_issued, 'YYYY-MM'), c.full_name
    ORDER BY month, total_value DESC
    LIMIT 5;
    """
    df_top5_high = stats_month(config_path, top5_high_query, 'invoices',
                                      'top5_high_invoices_per_month.csv')
    # print("Top 5 customers with most invoice value per month:")
    # print(df_top5_high)
    df_top5_high_agg = df_top5_high.groupby('month')['total_value'].sum().reset_index()
    plot_years_wp(df_top5_high_agg, years, 'Total Invoice Value for Top 5 Clients Per Month by Year', 'Month',
                  'Total Invoice Value', 'multiple_top5_clients')

    # Example 6: Top 5 clients with least invoice value total per month
    top5_low_query = """
    SELECT 
        TO_CHAR(i.date_issued, 'YYYY-MM') AS month,
        c.full_name,
        SUM(i.total) AS total_value
    FROM invoices i
    JOIN clients c ON i.full_name = c.full_name
    GROUP BY TO_CHAR(i.date_issued, 'YYYY-MM'), c.full_name
    HAVING SUM(i.total) > 0
    ORDER BY month, total_value ASC
    LIMIT 5;
    """
    df_top5_low = stats_month(config_path, top5_low_query, 'invoices_clients',
                                     'top5_low_invoices_per_month.csv')
    # print("Top 5 clients with least invoice value per month:")
    # print(df_top5_low)
    df_top5_low_agg = df_top5_low.groupby('month')['total_value'].sum().reset_index()
    plot_years_wp(df_top5_low_agg, years, 'Total Invoice Value for Bottom 5 Clients Per Month by Year', 'Month',
                  'Total Invoice Value', 'multiple_bottom5_clients')
