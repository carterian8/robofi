#-------------------------------------------------------------------------------
# System Imports
#-------------------------------------------------------------------------------

from datetime import datetime, timedelta
import math
from tracemalloc import start
import pytz
import sqlite3
from torch.utils.data import Dataset
from torch.utils.data import DataLoader
import yfinance as yf

#-------------------------------------------------------------------------------
# Local Imports
#-------------------------------------------------------------------------------

from ..utils import logger

################################################################################
# ...
#
def main(config, cl_args, logr):
    """
    """

    # Create a ticker dataset for each of the tickers in the config file

################################################################################
# ...
#
class TickerCache():
    """
    """

    def __init__(
        self,
        fname=":memory:"
    ):
        # Open the connection and create the table for ticker history data

        self.con = sqlite3.connect(fname)
        self.table_name = "tickers"
        cur = self.con.cursor()
        cur.execute(""" CREATE TABLE IF NOT EXISTS tickers 
            (Date string primary key, Open real, High real, Low real, 
            Close real, Volume real)
        """)
        self.con.commit()

    def __del__(self):
        # Save (commit) the changes
        self.con.commit()
        self.con.close()
    
    def insert(self, df):
        cur = self.con.cursor()

        df.to_sql(self.table_name, self.con, if_exists="append")

        # Save (commit) the changes
        self.con.commit()

    def retrieve(self, start_datetime, end_datetime):
        cur = self.con.cursor()
        fmt = "%Y-%m-%d %H:%M:%S"
        start_str = start_datetime.strftime(fmt)
        end_str = end_datetime.strftime(fmt)
        return cur.execute(
            """ 
            SELECT * 
            FROM tickers 
            WHERE Date BETWEEN ? AND ?
            ORDER BY Date
            """, 
            (start_str, end_str)
        ).fetchall()

################################################################################
# ...
#
class TickerDataset(Dataset):
    """
    """

    # Maps trading interval string into a seconds
    trading_intervals_to_secs = {
        "1m"  : 60,
        "2m"  : 120, 
        "5m"  : 300, 
        "15m" : 900, 
        "30m" : 1800, 
        "60m" : 3600, 
        "90m" : 5400, 
        "1h"  : 3600, 
        "1d"  : 86400, 
        "5d"  : 432000, 
        "1wk" : 604800, 
        "1mo" : 2419200, 
        "3mo" : 7257600
    }

    def __init__(
        self,
        ticker_name,
        start_date,
        end_date,
        trading_interval,
        win_sz,
        win_step,
        logr,
        transform=None, 
        target_transform=None
    ):
        """
        """
        
        self.cache = TickerCache()
        self.ticker_name = ticker_name
        self.ticker = yf.Ticker(ticker_name)
        self.trading_interval_str = trading_interval
        self.win_sz = win_sz
        self.win_step = win_step
        self.logr = logr
        self.transform = transform
        self.target_transform = target_transform

        # First validate the start/end dates and the trading interval

        fmt = "%Y-%m-%d"
        self.start_datetime = datetime.strptime(start_date, fmt)
        end_datetime = datetime.strptime(end_date, fmt)
        if self.start_datetime >= end_datetime:
            logr.error("Start date '{}' >= end date '{}'")
        

        # Determine how many OHLCV samples there are in given period with the
        # given trading interval

        time_delta = end_datetime - self.start_datetime
        # Convert the trading interval to seconds...
        self.trading_interval_sec = TickerDataset.trading_intervals_to_secs[
            trading_interval.strip().lower()
        ]
        num_ohlcv = math.floor(
            time_delta.total_seconds() / self.trading_interval_sec
        )

        # Calculate the number of samples in this dataset
        self.num_samples = math.floor((num_ohlcv - win_sz) / win_step) + 1

        self.logr.info(
            "TickerDataset: ticker_name '{}', start_date '{}', end_date '{}', "
            "trading_interval: '{}', num_ohlcv: '{}', win_sz: '{}, "
            "win_step: '{}', num_samples: '{}'".format(
                ticker_name, start_date, end_date, trading_interval, num_ohlcv,
                win_sz, win_step, self.num_samples
        ))

    def __len__(self):
        return self.num_samples

    def __getitem__(self, idx):

        # Get the starting index of the OHLCV record for the given window index
        win_start_datetime = self.start_datetime + timedelta(
            seconds=(self.win_step * self.trading_interval_sec * idx)
        )
        win_end_datetime = win_start_datetime + timedelta(
            seconds=(self.win_sz * self.trading_interval_sec)
        )

        # Check if the data has already been downloaded 

        samples = self.cache.retrieve(win_start_datetime, win_end_datetime)
        if len(samples) > 0:
            # Be sure to fetch any gaps around the cached data
            date_idx = 0
            fmt = "%Y-%m-%d %H:%M:%S"

            # Check if we're missing any samples at the start of our window

            cached_start_datetime = datetime.strptime(
                samples[0][date_idx], 
                fmt
            )
            if win_start_datetime < cached_start_datetime:
                # Fetch the samples missing at the beginning of the cache
                end_datetime = cached_start_datetime - timedelta(
                    seconds=self.trading_interval_sec
                )
                self.__fetch_and_insert__(win_start_datetime, end_datetime)
                

            # Check if we're missing any samples at the end of our window

            cached_end_datetime = datetime.strptime(
                samples[-1][date_idx], 
                fmt
            )
            if cached_end_datetime < win_end_datetime:
                # Fetch the samples missing at the end of the cache
                start_datetime = cached_end_datetime + timedelta(
                    seconds=self.trading_interval_sec
                )
                self.__fetch_and_insert__(start_datetime, win_end_datetime)
        else: 
            # Cache doesn't have any of the samples we need, fetch the whole 
            # window
            self.__fetch_and_insert(win_start_datetime, win_end_datetime)
        
        # All the samples should be in the cache now, grab them and convert them
        # into the proper type/format

        samples = self.cache.retrieve(win_start_datetime, win_end_datetime)

    def __fetch_and_insert__(self, start_datetime, end_datetime):
        """
        """
        # Fetch data from yahoo and insert into the cache
        df = self.ticker.history(
            start=start_datetime,
            end=end_datetime,
            interval=self.trading_interval_str
        )
        self.cache.insert(df[["Open", "High", "Low", "Close", "Volume"]])