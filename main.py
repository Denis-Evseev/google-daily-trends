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

    keyword = 'iphone'
    pytrend = TrendReq(hl='en-US')
    google_trends = GoogleTrends(pytrend, verbose=True)
    graph_builder = GraphBuilder()

    geo = 'US'
    start = "2017-01-01" #"2017-07-01"
    end = "2017-12-31" #"2017-12-31" "2019-11-23"

    # Getting all keywords:
    # df = pd.read_csv('search_df.csv')

    d = {'ticker': [keyword], 'keyword': [keyword]}
    df = pd.DataFrame(data=d)

    # Processing every keyword separately
    for index, row in df.iterrows():

        ticker = row['ticker']
        keyword = row['keyword']

        logger.info("Processing Ticker: " + ticker + " Keyword: " + keyword)
        overlapped_daily_data = google_trends.get_overlapped_daily_trend_data(keyword=keyword, start=start, end=end, geo=geo)
        daily_data = google_trends.get_daily_trend_data(keyword=keyword, start=start, end=end, geo=geo) # using dailydata in pytrends

        tf = start + ' ' + end
        geo = 'US'
        pytrend.build_payload(kw_list=[keyword], cat=0, geo=geo, gprop='', timeframe=tf)
        daily_real = pytrend.interest_over_time()

        logger.info("Finished processing...")

        filename = ticker + " " + execution_timestamp + ".csv"
        # daily_fetched_data.to_csv(filename)

        overlapped_daily_line = GraphLine(overlapped_daily_data[keyword], "overlapped daily", color='b')
        # daily_data_line = GraphLine(daily_data[keyword], "daily data", color='y')
        daily_real_line = GraphLine(daily_real[keyword], 'original data', color='m')
        overlap_line = GraphLine(overlapped_daily_data['overlap'], 'overlap', color='r')

        # Build chart - Way 1
        # overlapped_daily_data.plot()

        # Build chart - Way 2
        lines = [overlapped_daily_line, daily_real_line, overlap_line]
        graph_builder.build(lines, "dates", "Relative Search Trends", title="Daily Google Trends for keyword: " + keyword + " " + start + " " + end)

        logger.info("Build graph for : "  + ticker + " Keyword: " + keyword)


if __name__ == "__main__":
    main()


