import pandas as pd
from datetime import datetime, timedelta, date, time

from Utils.logger import Logger
from Utils.graph_builder import GraphBuilder
from google_trends import GoogleTrends

from Models.graph_line import GraphLine

import logging
from pytrends.request import TrendReq


def main():
    now = datetime.now()
    execution_timestamp = now.strftime("%d-%m-%Y %H-%M")

    logging.basicConfig(filename='google_trends ' + execution_timestamp + '.log', level=logging.INFO)
    logger = logging.getLogger("main")

    pytrend = TrendReq(hl='en-US', tz=360)
    google_trends = GoogleTrends(pytrend, verbose=True)
    graph_builder = GraphBuilder()

    # Getting all keywords:
    df = pd.read_csv('search_df.csv')

    # Processing every keyword separately
    for index, row in df.iterrows():

        ticker = row['ticker']
        keyword = row['keyword']

        logger.info("Processing Ticker: " + ticker + " Keyword: " + keyword)
        daily_fetched_data = google_trends.get_daily_trend(keyword=keyword, start="2020-01-01", end="2021-05-01")
        logger.info("Finished processing...")

        filename = ticker + " " + execution_timestamp + ".csv"
        # daily_fetched_data.to_csv(filename)

        daily_line = GraphLine(daily_fetched_data[keyword], "daily")
        overlap_line = GraphLine(daily_fetched_data['overlap'], 'overlap')
        lines = [daily_line, overlap_line]

        graph_builder.build(lines, "dates", "Relative Search Trends", title="Daily Google Trends for keyword: " + keyword)
        logger.info("Saved results to : " + filename)


if __name__ == "__main__":
    main()


