import pandas as pd
import os
import time
import re
from datetime import datetime
from utilities import data_string_to_float
from tqdm import tqdm

statspath = "intraQuarter/_KeyStats/"

features = [
    "Market Cap",
    "Enterprise Value",
    "Trailing P/E",
    "Forward P/E",
    "PEG Ratio",
    "Price/Sales",
    "Price/Book",
    "Enterprise Value/Revenue",
    "Enterprise Value/EBITDA",
    "Profit Margin",
    "Operating Margin",
    "Return on Assets",
    "Return on Equity",
    "Revenue",
    "Revenue Per Share",
    "Qtrly Revenue Growth",
    "Gross Profit",
    "EBITDA",
    "Net Income Avl to Common",
    "Diluted EPS",
    "Qtrly Earnings Growth",
    "Total Cash",
    "Total Cash Per Share",
    "Total Debt",
    "Total Debt/Equity",
    "Current Ratio",
    "Book Value Per Share",
    "Operating Cash Flow",
    "Levered Free Cash Flow",
    "Beta",
    "50-Day Moving Average",
    "200-Day Moving Average",
    "Avg Vol (3 month)",
    "Shares Outstanding",
    "Float",
    "% Held by Insiders",
    "% Held by Institutions",
    "Shares Short (as of",
    "Short Ratio",
    "Short % of Float",
    "Shares Short (prior month",
]


def preprocess_price_data():

    sp500_raw_data = pd.read_csv("data/sp_index.csv", index_col="Date", parse_dates=True)
    stock_raw_data = pd.read_csv("data/stock_prices.csv", index_col="Date", parse_dates=True)

    start_date = str(stock_raw_data.index[0])
    end_date = str(stock_raw_data.index[-1])
    idx = pd.date_range(start_date, end_date)
    sp500_raw_data = sp500_raw_data.reindex(idx)
    stock_raw_data = stock_raw_data.reindex(idx)

    sp500_raw_data.ffill(inplace=True)
    stock_raw_data.ffill(inplace=True)

    return sp500_raw_data, stock_raw_data


def parse_keystats(sp500_df, stock_df):

    stock_list = [x[0] for x in os.walk(statspath)]
    stock_list = stock_list[1:]

    df_columns = [
        "Date",
        "Unix",
        "Ticker",
        "Price",
        "stock_p_change",
        "SP500",
        "SP500_p_change",
    ] + features

    df = pd.DataFrame(columns=df_columns)

    for stock_directory in tqdm(stock_list, desc="Parsing progress:", unit="tickers"):
        keystats_html_files = os.listdir(stock_directory)

        if ".DS_Store" in keystats_html_files:
            keystats_html_files.remove(".DS_Store")

        ticker = stock_directory.split(statspath)[1]

        for file in keystats_html_files:

            date_stamp = datetime.strptime(file, "%Y%m%d%H%M%S.html")
            unix_time = time.mktime(date_stamp.timetuple())

            full_file_path = stock_directory + "/" + file

            value_list = []

            with open(full_file_path, "r") as source:
                source = source.read()
                source = source.replace(",", "")

                for variable in features:
                    try:
                        regex = (
                            r">"
                            + re.escape(variable)
                            + r".*?(\-?\d+\.*\d*K?M?B?|N/A[\\n|\s]*|>0|NaN)%?"
                            r"(</td>|</span>)"
                        )
                        value = re.search(regex, source, flags=re.DOTALL).group(1)

                        value_list.append(data_string_to_float(value))

                    except AttributeError:
                        if variable == "Avg Vol (3 month)":
                            try:
                                new_variable = ">Average Volume (3 month)"
                                regex = (
                                    re.escape(new_variable)
                                    + r".*?(\-?\d+\.*\d*K?M?B?|N/A[\\n|\s]*|>0)%?"
                                    r"(</td>|</span>)"
                                )
                                value = re.search(regex, source, flags=re.DOTALL).group(
                                    1
                                )
                                value_list.append(data_string_to_float(value))
                            except AttributeError:
                                value_list.append("N/A")
                        else:
                            value_list.append("N/A")

            current_date = datetime.fromtimestamp(unix_time).strftime("%Y-%m-%d")
            one_year_later = datetime.fromtimestamp(unix_time + 31536000).strftime(
                "%Y-%m-%d"
            )

            sp500_price = float(sp500_df.loc[current_date, "Adj Close"])
            sp500_1y_price = float(sp500_df.loc[one_year_later, "Adj Close"])
            sp500_p_change = round(
                ((sp500_1y_price - sp500_price) / sp500_price * 100), 2
            )

            stock_price, stock_1y_price = "N/A", "N/A"
            try:
                stock_price = float(stock_df.loc[current_date, ticker.upper()])
                stock_1y_price = float(stock_df.loc[one_year_later, ticker.upper()])
            except KeyError:
                continue

            stock_p_change = round(
                ((stock_1y_price - stock_price) / stock_price * 100), 2
            )

            new_df_row = [
                date_stamp,
                unix_time,
                ticker,
                stock_price,
                stock_p_change,
                sp500_price,
                sp500_p_change,
            ] + value_list

            df = df.append(dict(zip(df_columns, new_df_row)), ignore_index=True)

    df.dropna(axis=0, subset=["Price", "stock_p_change"], inplace=True)
    df.to_csv("data/ks.csv", index=False)


if __name__ == "__main__":
    sp500_df, stock_df = preprocess_price_data()
    parse_keystats(sp500_df, stock_df)
