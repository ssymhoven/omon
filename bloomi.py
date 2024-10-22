import os
import json
from datetime import datetime, timedelta

import blpapi
import pandas as pd


class BloombergSource:
    def __init__(self):
        self.session = blpapi.Session()
        session_options = blpapi.SessionOptions()
        session_options.setServerHost("localhost")
        session_options.setServerPort(8194)
        self.session.start()

    def __enter__(self):
        self.session.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.stop()

    def _send_request(self, request):
        self.session.sendRequest(request)
        data = []
        while True:
            event = self.session.nextEvent()
            if event.eventType() in [blpapi.Event.RESPONSE, blpapi.Event.PARTIAL_RESPONSE]:
                for msg in event:
                    if msg.hasElement("securityData"):
                        data.append(msg.getElement("securityData"))
            if event.eventType() == blpapi.Event.RESPONSE:
                break
        return data

    def fetch_data_for_securities(self, series_ids: list, fields: list = None) -> list:
        """
        Fetches data for a batch of securities with specified fields.

        Args:
            series_ids (list): List of Bloomberg tickers for the securities.
            fields (list): List of fields to request.

        Returns:
            list: A list of dictionaries with fetched values for each security.
        """
        if not self.session.openService("//blp/refdata"):
            raise Exception("Failed to open //blp/refdata service")

        # Prepare a single request for all series IDs
        request = self.session.getService("//blp/refdata").createRequest("ReferenceDataRequest")
        for series_id in series_ids:
            request.getElement("securities").appendValue(series_id)

        for field in fields:
            request.getElement("fields").appendValue(field)

        # Send the request and process the response
        data = self._send_request(request)

        # Extract the relevant fields from the response
        security_data = []
        for element in data:
            for sec_data in element:
                security_name = sec_data.getElement("security").getValue()
                opt_data = {"SECURITY": security_name}
                for field in fields:
                    if sec_data.hasElement("fieldData") and sec_data.getElement("fieldData").hasElement(field):
                        if field == "OPT_CHAIN":
                            opt_data[field] = []
                            field_data = sec_data.getElement("fieldData").getElement(field)
                            for i in range(field_data.numValues()):
                                option = field_data.getValueAsElement(i).getElementAsString("Security Description")
                                opt_data[field].append(option)
                        else:
                            if field == 'EXPIRATION_PERIODICITY':
                                opt_data[field] = sec_data.getElement("fieldData").getElementAsString(field)
                            else:
                                opt_data[field] = sec_data.getElement("fieldData").getElementAsFloat(field)
                security_data.append(opt_data)
        return security_data


def filter_option_chains(security_data: list) -> pd.DataFrame:
    """
    Filters the option chains based on the criteria: only Call options with settlement on the third Friday of next month
    and a strike price higher than the PX_LAST of the security.
    """
    next_month = (datetime.now() + timedelta(days=30)).month
    next_year = datetime.now().year if next_month != 1 else datetime.now().year + 1

    third_friday = get_third_friday(next_year, next_month)

    filtered_options = []
    for sec in security_data:
        px_last = sec.get("PX_LAST")
        if px_last is None:
            continue

        for option in sec.get("OPT_CHAIN", []):
            try:
                parts = option.split()
                if "C" in parts[-2] and sec["SECURITY"] != "SPX Index":
                    expiry_date = datetime.strptime(parts[2], "%m/%d/%y")
                    strike_price = float(parts[-2][1:])

                    if expiry_date == third_friday and strike_price > px_last:
                        filtered_options.append({
                            "SECURITY": sec["SECURITY"],
                            "OPTION": option,
                            "TYPE": "CALL",
                            "PX_LAST": px_last,
                            "STRIKE_PRICE": strike_price,
                            "EXPIRY_DATE": expiry_date
                        })

                if "P" in parts[-2] and sec["SECURITY"] == "SPX Index" and "SPXW" not in option:
                    expiry_date = datetime.strptime(parts[2], "%m/%d/%y")
                    strike_price = float(parts[-2][1:])

                    if expiry_date == third_friday and strike_price < px_last:
                        filtered_options.append({
                            "SECURITY": sec["SECURITY"],
                            "OPTION": option,
                            "TYPE": "PUT",
                            "PX_LAST": px_last,
                            "STRIKE_PRICE": strike_price,
                            "EXPIRY_DATE": expiry_date
                        })

            except Exception as e:
                print(f"Error processing option {option}: {e}")

    df_filtered_options = pd.DataFrame(filtered_options)

    return df_filtered_options


def get_third_friday(year: int, month: int) -> datetime:
    """
    Get the third Friday of the given month and year.
    """
    first_day_of_month = datetime(year, month, 1)
    third_friday = first_day_of_month + timedelta(weeks=2)
    third_friday += timedelta(days=(4 - third_friday.weekday()))  # Adjust to the next Friday (weekday 4)
    return third_friday


def find_nearest_otm_option(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters the options data based on DELTA, OPEN_INT, and finds the option with the strike price
    nearest to 10% out-of-the-money (OTM) for each security.
    Adds the TARGET_STRIKE and calculates moneyness (STRIKE_PRICE / PX_LAST) for the selected option,
    ensuring the option is above 10% moneyness.

    Args:
        df (pd.DataFrame): The filtered options DataFrame with DELTA, OPEN_INT, STRIKE_PRICE, and PX_LAST columns.

    Returns:
        pd.DataFrame: DataFrame containing the closest options to 10% OTM for each security,
                      with TARGET_STRIKE and Moneyness columns.
    """
    df_filtered = df[((df['DELTA'] >= 0.15) & (df['DELTA'] <= 0.5) | (df['DELTA'] <= -0.05) & (df['DELTA'] >= -0.5)) & (df['OPEN_INT'] >= 100)]

    def find_closest_option(group):
        px_last = group['PX_LAST'].iloc[0]
        security = group['SECURITY_x'].iloc[0]

        if security == "SPX Index":
            target_strike = px_last * 0.9
            option_type = 'P'
        else:
            target_strike = px_last * 1.1
            option_type = 'C'

        group['TARGET_STRIKE'] = target_strike
        group['Moneyness'] = ((group['STRIKE_PRICE'] / px_last) - 1)

        if option_type == 'C':
            group_filtered = group[(group['Moneyness'] >= 0.05) & group['OPTION'].str.contains(option_type)]
        else:
            group_filtered = group[(group['Moneyness'] >= -0.1) & group['OPTION'].str.contains(option_type)]

        if not group_filtered.empty:
            group_filtered['Strike_Diff'] = (group_filtered['STRIKE_PRICE'] - target_strike).abs()
            closest_option = group_filtered.loc[group_filtered['Strike_Diff'].idxmin()]

            return closest_option
        else:
            print(f"No option found for {security}.")
            return None

    df_closest_options = df_filtered.groupby('SECURITY_x').apply(find_closest_option).dropna()
    df_closest_options = df_closest_options.drop(columns=['Strike_Diff'], errors='ignore')

    return df_closest_options


def fetch_data_for_portfolio(portfolio_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fetches data for all securities in the portfolio, filters it, requests additional fields for options, and saves the result.

    Args:
        portfolio_df (pd.DataFrame): Dataframe containing a 'series_id' column with Bloomberg tickers.

    Returns:
        None
    """
    output_file = f"bloomberg_data.json"
    filtered_output_file = f"filtered_bloomberg_data.xlsx"
    option_data_input_file = f"option_input_data.xlsx"

    bloomberg = BloombergSource()

    if os.path.exists(output_file):
        with open(output_file, 'r') as infile:
            security_data = json.load(infile)
        print(f"Data loaded from {output_file}")
    else:
        series_ids = portfolio_df.index.tolist()
        series_ids.append("SPX Index")
        security_data = bloomberg.fetch_data_for_securities(series_ids,
                                                            fields=["PX_LAST", "OPT_CHAIN", "VOLATILITY_30D",
                                                                    "CALL_IMP_VOL_30D"])

        with open(output_file, 'w') as outfile:
            json.dump(security_data, outfile, indent=4)
        print(f"Data successfully saved to {output_file}")

    if os.path.exists(option_data_input_file):
        print(f"Option data already exists at {option_data_input_file}")
        df = pd.read_excel(option_data_input_file)
    else:
        df_filtered_options = filter_option_chains(security_data)
        option_ids = df_filtered_options["OPTION"].tolist()

        if option_ids:
            option_data = bloomberg.fetch_data_for_securities(option_ids,
                                                              fields=["DELTA", "GAMMA", "PX_ASK", "PX_BID", "EXPIRATION_PERIODICITY", "PRICE_MULTIPLIER", "OPEN_INT"])

            df_option_data = pd.DataFrame(option_data)

            df = pd.merge(df_filtered_options, df_option_data, left_on="OPTION", right_on="SECURITY", how="left")
            df.to_excel(option_data_input_file)

    if os.path.exists(filtered_output_file):
        print(f"Filtered data already exists at {filtered_output_file}")
        df = pd.read_excel(filtered_output_file, index_col=0, header=0)
    else:
        df = find_nearest_otm_option(df)
        df.to_excel(filtered_output_file)
        print(f"Filtered data with additional fields saved to {filtered_output_file}")

    return df
