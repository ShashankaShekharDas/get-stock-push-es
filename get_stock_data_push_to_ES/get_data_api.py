import os
from datetime import datetime

import yfinance as yf

# Importing Constants
from get_stock_data_push_to_ES import GET_DATA_PERIOD_MINUTES, DATA_DIRECTORY


# GET_DATA_PERIOD_MINUTES = 10

class Get_stock_data:
    """
        Class to Get Stock Data from yahoo finance
        And write data to folder to be sent to ES
    """

    def __init__(self):
        self.tickers = [
            "^BSESN",
            "^NSEI",
            "BTC-USD",
            "ETH-USD"
        ]
        self.data_extension = ".csv"

        # Set period and interval to get the data
        self.period = str(GET_DATA_PERIOD_MINUTES) + "m"
        self.interval = "1m"

    def get_stock_data(self):
        if not os.path.exists(DATA_DIRECTORY):
            os.makedirs(DATA_DIRECTORY)

        for ticker in self.tickers:
            file_path = os.path.join(DATA_DIRECTORY, ticker+str(int(datetime.now().timestamp()))+self.data_extension)
            print(file_path)
            history_ticker = yf.Ticker(ticker).history(period=self.period, interval=self.interval)
            history_ticker.to_csv(file_path, sep='\t', encoding='utf-8')

#
# Get_stock_data().get_stock_data()
