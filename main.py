import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt
from pandas.tseries.offsets import Week
from source_engine.opus_source import OpusSource
from tabulate import tabulate

from bloomi import fetch_data_for_portfolio

query = """
    SELECT
            positions.bloomberg_query,
            positions.name,
            positions.volume,
            positions.last_quote,
            positions.last_xrate_quantity,
            accountsegment.predicted_nav,
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
"""

opus = OpusSource()


def get_portfolio() -> pd.DataFrame:
    df = opus.read_sql(query=query)
    df["value"] = df["last_quote"] * df["last_xrate_quantity"] * df["volume"]
    df["percent_nav"] = df["value"] / df["predicted_nav"]
    df.set_index("bloomberg_query", inplace=True)
    return df


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

    df = pd.read_excel(file_name, sheet_name=sheet_name, index_col=0, parse_dates=True)
    df.dropna(inplace=True)

    process_and_generate_plots(df)


def calculate_premium(row):
    """
    Calculate the premium for an option based on its type (CALL or PUT).
    For CALL options, use PX_BID. For PUT options, use PX_ASK.

    Args:
        row (pd.Series): A row of the DataFrame containing option data.

    Returns:
        float: The calculated premium.
    """
    if row['TYPE'] == 'CALL':
        premium = (row['PX_BID'] / row['PX_LAST']) * row['percent_nav'] * 100 * 100
    elif row['TYPE'] == 'PUT':
        premium = - (row['PX_ASK'] / row['PX_LAST']) * 100 * 100
    else:
        premium = None

    return premium


def generate_metrics(df: pd.DataFrame):

    call_premium_sum = df.loc[df['TYPE'] == 'CALL', 'Premium'].sum()
    put_premium_sum = df.loc[df['TYPE'] == 'PUT', 'Premium'].sum()

    put_call_spread = call_premium_sum + put_premium_sum

    call_percent_nav_sum = round(df.loc[df['TYPE'] == 'CALL', 'percent_nav'].sum() * 100, 2)

    metrics = {
        "Total CALL Premium (BPS)": call_premium_sum,
        "Total PUT Premium (BPS)": put_premium_sum,
        "PUT/CALL Spread (BPS)": put_call_spread,
        "Total CALL Percent NAV (%)": call_percent_nav_sum,
        "Total PUT Percent NAV (%)": 100.00
    }

    metrics_df = pd.DataFrame([metrics])
    return metrics_df


if __name__ == "__main__":
    plot_histogram()

    port = get_portfolio()
    options = fetch_data_for_portfolio(port)
    merged_df = port.merge(options, left_index=True, right_index=True, how='outer')
    merged_df['Premium'] = merged_df.apply(calculate_premium, axis=1)

    merged_df.sort_values(by='percent_nav', inplace=True)
    metrics = generate_metrics(df=merged_df)
    print(tabulate(metrics, headers='keys', tablefmt='psql'))

    output_file = 'port.xlsx'
    merged_df.to_excel(output_file, index=False)
