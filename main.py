import pandas as pd
from datetime import datetime, timedelta, date, time

from Utils.logger import Logger
from google_trends import GoogleTrends

import logging
from pytrends.request import TrendReq


def main():
    now = datetime.now()
    execution_timestamp = now.strftime("%d-%m-%Y %H-%M")

    logging.basicConfig(filename='google_trends ' + execution_timestamp + '.log', level=logging.INFO)
    logger = logging.getLogger("main")

    pytrend = TrendReq(hl='en-US', tz=360)
    google_trends = GoogleTrends(pytrend, verbose=True)

    # Getting all keywords:
    df = pd.read_csv('search_df.csv')

    # Processing every keyword separately
    for index, row in df.iterrows():

        logger.info("Processing Ticker: " + row['ticker'] + " Keyword: " + row['keyword'])
        daily_fetched_data = google_trends.get_daily_trend(keyword=row['keyword'], start="2020-01-01", end="2021-05-01")
        logger.info("Finished processing...")

        filename = row['ticker'] + " " + execution_timestamp + ".csv"
        daily_fetched_data.to_csv(filename)
        logger.info("Saved results to : " + filename)


if __name__ == "__main__":
    main()


