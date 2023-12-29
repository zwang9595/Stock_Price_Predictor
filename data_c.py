import pandas as pd
import os
import re
import time
import requests
import numpy as np
from tqdm import tqdm
from utilities import data_string_to_float

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
    "Quarterly Revenue Growth",
    "Gross Profit",
    "EBITDA",
    "Net Income Avi to Common",
    "Diluted EPS",
    "Quarterly Earnings Growth",
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
    "Shares Short",
    "Short Ratio",
    "Short % of Float",
    "Shares Short (prior month",
]


def check_yahoo():

    if not os.path.exists("forward/"):
        os.makedirs("forward/")

    ticker_list = os.listdir(statspath)

    if ".DS_Store" in ticker_list:
        ticker_list.remove(".DS_Store")

    for ticker in tqdm(ticker_list, desc="Download progress:", unit="tickers"):
        try:
            link = f"http://finance.yahoo.com/quote/{ticker.upper()}/key-statistics"
            resp = requests.get(link)

            save = f"forward/{ticker}.html"
            with open(save, "w", encoding='utf-8') as file:
                file.write(resp.text)

        except Exception as e:
            print(f"{ticker}: {str(e)}\n")
            time.sleep(2)


def forward():

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

    tickerfile_list = os.listdir("forward/")

    if ".DS_Store" in tickerfile_list:
        tickerfile_list.remove(".DS_Store")

    for tickerfile in tqdm(tickerfile_list, desc="Parsing progress:", unit="tickers"):
        ticker = tickerfile.split(".html")[0].upper()
        source = open(f"forward/{tickerfile}", encoding='utf-8').read()

        source = source.replace(",", "")

        value_list = []
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
                value_list.append("N/A")

        new_df_row = [0, 0, ticker, 0, 0, 0, 0] + value_list

        df = df.append(dict(zip(df_columns, new_df_row)), ignore_index=True)

    return df.replace("N/A", np.nan)


if __name__ == "__main__":
    check_yahoo()
    current_df = forward()
    current_df.to_csv("data/fs.csv", index=False)
