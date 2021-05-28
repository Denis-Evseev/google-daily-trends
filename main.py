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

    start = "2017-01-01" #"2020-01-01"
    end = "2019-11-23" #"2021-05-01"

    # Getting all keywords:
    # df = pd.read_csv('search_df.csv')

    d = {'ticker': ["iPhone"], 'keyword': ["iPhone"]}
    df = pd.DataFrame(data=d)

    # Processing every keyword separately
    for index, row in df.iterrows():

        ticker = row['ticker']
        keyword = row['keyword']

        logger.info("Processing Ticker: " + ticker + " Keyword: " + keyword)
        overlapped_daily_data = google_trends.get_overlapped_daily_trend_data(keyword=keyword, start=start, end=end)
        original_daily_data = google_trends.get_original_daily_trend_data(keyword=keyword, start=start, end=end)
        logger.info("Finished processing...")

        filename = ticker + " " + execution_timestamp + ".csv"
        # daily_fetched_data.to_csv(filename)

        overlapped_daily_line = GraphLine(overlapped_daily_data[keyword], "overlapped daily")
        original_daily_line = GraphLine(original_daily_data[keyword], "original daily")
        overlap_line = GraphLine(overlapped_daily_data['overlap'], 'overlap')

        # Build chart - Way 1
        # overlapped_daily_data.plot()

        # Build chart - Way 2
        lines = [overlapped_daily_line, original_daily_line, overlap_line]
        graph_builder.build(lines, "dates", "Relative Search Trends", title="Daily Google Trends for keyword: " + keyword + " " + start + " " + end)

        logger.info("Build graph for : "  + ticker + " Keyword: " + keyword)


if __name__ == "__main__":
    main()


