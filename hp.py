import os
from pandas_datareader import data as pdr
import pandas as pd
import fix_yahoo_finance as yf

yf.pdr_override()

START_DATE = "2003-08-01"
END_DATE = "2015-01-01"

def build_stock_dataset(start=START_DATE, end=END_DATE):

    statspath = "intraQuarter/_KeyStats/"
    ticker_list = os.listdir(statspath)

    if ".DS_Store" in ticker_list:
        os.remove(f"{statspath}/.DS_Store")
        ticker_list.remove(".DS_Store")

    all_data = pdr.get_data_yahoo(ticker_list, start, end)
    stock_data = all_data["Adj Close"]

    stock_data.dropna(how="all", axis=1, inplace=True)
    missing_tickers = [
        ticker for ticker in ticker_list if ticker.upper() not in stock_data.columns
    ]
    print(f"{len(missing_tickers)} tickers are missing: \n {missing_tickers} ")

    stock_data.ffill(inplace=True)
    stock_data.to_csv("data/stock_prices.csv")


def build_sp500_dataset(start=START_DATE, end=END_DATE):

    index_data = pdr.get_data_yahoo("SPY", start=START_DATE, end=END_DATE)
    index_data.to_csv("data/sp_index.csv")


def build_dataset_iteratively(
    idx_start, idx_end, date_start=START_DATE, date_end=END_DATE
):

    statspath = "intraQuarter/_KeyStats/"
    ticker_list = os.listdir(statspath)

    df = pd.DataFrame()

    for ticker in ticker_list:
        ticker = ticker.upper()

        stock_ohlc = pdr.get_data_yahoo(ticker, start=date_start, end=date_end)
        if stock_ohlc.empty:
            print(f"No data for {ticker}")
            continue
        adj_close = stock_ohlc["Adj Close"].rename(ticker)
        df = pd.concat([df, adj_close], axis=1)
    df.to_csv("data/stock_prices.csv")


if __name__ == "__main__":
    build_stock_dataset()
    build_sp500_dataset()
