import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt
from pandas.tseries.offsets import Week
from source_engine.opus_source import OpusSource

from bloomi import fetch_data_for_portfolio

query = """
    SELECT
            positions.bloomberg_query,
            positions.name,
            positions.percent_nav
        FROM
            reportings
                JOIN
            accountsegments ON (accountsegments.reporting_uuid = reportings.uuid)
                JOIN
            positions ON (reportings.uuid = positions.reporting_uuid)
        WHERE
                positions.account_segment_id = accountsegments.accountsegment_id
                        AND accountsegments.accountsegment_id = '17154631'
                AND reportings.newest = 1
                AND reportings.report = 'positions'
                AND positions.asset_class = 'STOCK'
                AND positions.bloomberg_query is not null
                AND reportings.report_date = (SELECT
                                                MAX(report_date)
                                              FROM
                                                reportings)
    ORDER BY positions.percent_nav DESC
"""

opus = OpusSource()


def get_portfolio() -> pd.DataFrame:
    return opus.read_sql(query=query)


def generate_third_fridays(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DatetimeIndex:
    """
    Generates a list of third Fridays for each month between start_date and end_date.

    :param start_date: The start date of the period.
    :param end_date: The end date of the period.
    :return: A DatetimeIndex containing the third Fridays of each month.
    """
    third_fridays = []
    current_date = pd.Timestamp(start_date.year, start_date.month, 1)

    while current_date <= end_date:
        # Start from the first day of the month and find the third Friday
        first_friday = current_date + Week(weekday=4)  # Find the first Friday
        third_friday = first_friday + Week(2)  # Add two weeks to get the third Friday
        if third_friday <= end_date:
            third_fridays.append(third_friday)
        current_date += pd.DateOffset(months=1)  # Move to the next month

    return pd.DatetimeIndex(third_fridays)


def calculate_performance_third_friday(data: pd.Series, freq: str) -> pd.Series:
    """
    Calculate performance based on the third Friday of each month or quarter.

    :param data: A Series with timeseries data.
    :param freq: The frequency ('M' for monthly, 'Q' for quarterly).
    :return: A Series with the calculated performance.
    """
    # Generate third Fridays over the full time span of the data
    third_fridays = generate_third_fridays(data.index.min(), data.index.max())

    if freq == 'Q':
        third_fridays = third_fridays[::3]  # Select every third third Friday for quarterly

    # Filter third Fridays to match only the dates present in the data
    valid_third_fridays = third_fridays[third_fridays.isin(data.index)]

    # Calculate performance from one third Friday to the next
    performance = data.loc[valid_third_fridays].pct_change().dropna() * 100
    return performance


def plot_histograms_with_kde_subplots(monthly_data: pd.Series, quarterly_data: pd.Series, title: str):
    """
    Plots monthly and quarterly performance histograms with KDE in subplots (one row, two columns).

    :param monthly_data: A Series containing monthly performance data.
    :param quarterly_data: A Series containing quarterly performance data.
    :param title: The title for the subplot.
    """
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    sns.histplot(monthly_data, kde=True, bins=20, color='blue', ax=axes[0])
    axes[0].set_title(f'Monthly Returns of {title}')
    axes[0].set_xlabel('Returns (%)')
    axes[0].set_ylabel('Frequency')
    axes[0].grid(True)
    axes[0].xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))

    sns.histplot(quarterly_data, kde=True, bins=20, color='green', ax=axes[1])
    axes[1].set_title(f'Quarterly Returns of {title}')
    axes[1].set_xlabel('Returns (%)')
    axes[1].set_ylabel('Frequency')
    axes[1].grid(True)
    axes[1].xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f'{x:.0%}'))

    plt.tight_layout()
    plt.savefig(f'images/{title}.png')


def process_and_generate_plots(df: pd.DataFrame):
    """
    Processes each column in the dataframe, calculates performance, and generates charts.

    :param df: A DataFrame with timeseries data.
    """
    for column in df.columns:
        monthly_performance = calculate_performance_third_friday(df[column], 'M')
        quarterly_performance = calculate_performance_third_friday(df[column], 'Q')

        plot_histograms_with_kde_subplots(monthly_performance, quarterly_performance, column)


def plot_histogram():
    file_name = "option_monitor.xlsx"
    sheet_name = "Indices"

    # Step 1: Read data
    df = pd.read_excel(file_name, sheet_name=sheet_name, index_col=0, parse_dates=True)
    df.dropna(inplace=True)

    # Step 2: Process data and generate charts
    process_and_generate_plots(df)


if __name__ == "__main__":
    plot_histogram()

    port = get_portfolio()
    #maturity_dates = generate_third_fridays()
    options = fetch_data_for_portfolio(port)
