import pandas as pd
from datetime import datetime, timedelta, date
import time

from pytrends.exceptions import ResponseError
from pytrends import dailydata

import logging

class GoogleTrends:

    def __init__(self, trendreq, verbose=False):
        self.trendreq = trendreq
        self.verbose = verbose
        self.logger = logging.getLogger("GoogleTrends")

    def _fetch_data(self, kw_list, timeframe='today 3-m', cat=0, geo='', gprop='') -> pd.DataFrame:
        """Download google trends data using pytrends TrendReq and retries in case of a ResponseError."""
        attempts, fetched = 0, False
        while not fetched:
            try:
                self.trendreq.build_payload(kw_list=kw_list, timeframe=timeframe, cat=cat, geo=geo, gprop=gprop)
            except ResponseError as err:
                self.logger.error(err)
                self.logger.warning(f'Trying again in {60 + 5 * attempts} seconds.')
                time.sleep(60 + 5 * attempts)
                attempts += 1
                if attempts > 3:
                    self.logger.warning('Failed after 3 attemps, abort fetching.')
                    break
            else:
                fetched = True
        return self.trendreq.interest_over_time()

    def get_overlapped_daily_trend_data(self, keyword: str, start: str, end: str, cat=0,
                        geo='', gprop='', delta=269, overlap=100, sleep=0,
                        tz=0) -> pd.DataFrame:
        """Stitch and scale consecutive daily trends data between start and end date.
        This function will first download piece-wise google trends data and then
        scale each piece using the overlapped period.

            Parameters
            ----------
            trendreq : TrendReq
                a pytrends TrendReq object
            keyword: str
                currently only support single keyword, without bracket
            start: str
                starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
            end: str
                ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
            cat, geo, gprop, sleep:
                same as defined in pytrends
            delta: int
                The length(days) of each timeframe fragment for fetching google trends data,
                need to be <269 in order to obtain daily data.
            overlap: int
                The length(days) of the overlap period used for scaling/normalization
            tz: int
                The timezone shift in minute relative to the UTC+0 (google trends default).
                For example, correcting for UTC+8 is 480, and UTC-6 is -360

        """

        start_d = datetime.strptime(start, '%Y-%m-%d')
        init_end_d = end_d = datetime.strptime(end, '%Y-%m-%d')
        init_end_d.replace(hour=23, minute=59, second=59)
        delta = timedelta(days=delta)
        overlap = timedelta(days=overlap)

        itr_d = end_d - delta
        overlap_start = None

        df = pd.DataFrame()
        ol = pd.DataFrame()

        while end_d > start_d:
            tf = itr_d.strftime('%Y-%m-%d') + ' ' + end_d.strftime('%Y-%m-%d')
            if self.verbose: self.logger.info('Fetching \'' + keyword + '\' for period:' + tf)
            temp = self._fetch_data(pd.Series(data=[keyword]), timeframe=tf, cat=cat, geo=geo, gprop=gprop)

            if 'isPartial' in temp.columns:
                temp.drop(columns=['isPartial'], inplace=True)

            temp.columns.values[0] = tf
            ol_temp = temp.copy()
            ol_temp.iloc[:, :] = None

            if overlap_start is not None:  # not first iteration
                if self.verbose:
                    self.logger.info('Normalize by overlapping period:'
                                     + overlap_start.strftime('%Y-%m-%d') + ' - ' + end_d.strftime('%Y-%m-%d'))

                # normalize using the maximum value of the overlapped period
                y1 = temp.loc[overlap_start:end_d].iloc[:, 0].values.max()
                y2 = df.loc[overlap_start:end_d].iloc[:, -1].values.max()  # take the last column
                coef = y2 / y1
                temp = temp * coef
                ol_temp.loc[overlap_start:end_d, :] = 1

            df = pd.concat([df, temp], axis=1)  # concatenate data
            ol = pd.concat([ol, ol_temp], axis=1)

            # shift the timeframe for next iteration
            overlap_start = itr_d
            end_d -= (delta - overlap)
            itr_d -= (delta - overlap)
            # in case of short query interval getting banned by server
            time.sleep(sleep)

        df.sort_index(inplace=True)
        ol.sort_index(inplace=True)

        # The daily trend data is missing the most recent 3-days data, need to complete with hourly data
        if df.index.max() < init_end_d:
            tf = 'now 7-d'
            hourly = self._fetch_data([keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
            hourly.drop(columns=['isPartial'], inplace=True)

            # convert hourly data to daily data
            daily = hourly.groupby(hourly.index.date).sum()

            # check whether the first day data is complete (i.e. has 24 hours)
            daily['hours'] = hourly.groupby(hourly.index.date).count()
            if daily.iloc[0].loc['hours'] != 24: daily.drop(daily.index[0], inplace=True)
            daily.drop(columns='hours', inplace=True)

            daily.set_index(pd.DatetimeIndex(daily.index), inplace=True)
            daily.columns = [tf]

            ol_temp = daily.copy()
            ol_temp.iloc[:, :] = None

            # find the overlapping date
            intersect = df.index.intersection(daily.index)
            if self.verbose:
                self.logger.info('Normalize by overlapping period:' + (intersect.min().strftime('%Y-%m-%d'))
                      + ' ' + (intersect.max().strftime('%Y-%m-%d')))

            # scaling use the overlapped today-4 to today-7 data
            coef = df.loc[intersect].iloc[:, 0].max() / daily.loc[intersect].iloc[:, 0].max()
            daily = (daily * coef).round(decimals=0)
            ol_temp.loc[intersect, :] = 1

            df = pd.concat([daily, df], axis=1)
            ol = pd.concat([ol_temp, ol], axis=1)

        # taking averages for overlapped period
        df = df.mean(axis=1)
        ol = ol.max(axis=1)
        # merge the two dataframe (trend data and overlap flag)
        df = pd.concat([df, ol], axis=1)
        df.columns = [keyword, 'overlap']
        # Correct the timezone difference
        df.index = df.index + timedelta(minutes=tz)
        df = df[start_d:init_end_d]
        # re-normalized to the overall maximum value to have max =100
        df[keyword] = (100 * df[keyword] / df[keyword].max()).round(decimals=0)

        overlapped_daily_data = df
        return overlapped_daily_data


    def get_daily_trend_data(self, keyword: str, start: str, end: str, geo='', verbose=False):
        start_d = datetime.strptime(start, '%Y-%m-%d')
        end_d = datetime.strptime(end, '%Y-%m-%d')
        s_year = start_d.year
        s_mon = start_d.month
        e_year = end_d.year
        e_mon = end_d.month

        pytrends_daily_data = dailydata.get_daily_data(word=keyword,
                                             start_year=s_year,
                                             start_mon=s_mon,
                                             stop_year=e_year,
                                             stop_mon=e_mon,
                                             geo=geo,
                                             verbose=verbose,
                                             wait_time=1.0)
        return pytrends_daily_data


    def get_original_daily_trend_data(self, keyword: str, start: str, end: str, cat=0,
                        geo='', gprop='', delta=268, sleep=0, tz=0) -> pd.DataFrame:
        """Stitch and scale consecutive daily trends data between start and end date.
        This function will first download piece-wise google trends data and then
        scale each piece using the overlapped period.

            Parameters
            ----------
            trendreq : TrendReq
                a pytrends TrendReq object
            keyword: str
                currently only support single keyword, without bracket
            start: str
                starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
            end: str
                ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
            cat, geo, gprop, sleep:
                same as defined in pytrends
            delta: int
                The length(days) of each timeframe fragment for fetching google trends data,
                need to be <269 in order to obtain daily data.
            overlap: int
                The length(days) of the overlap period used for scaling/normalization
            tz: int
                The timezone shift in minute relative to the UTC+0 (google trends default).
                For example, correcting for UTC+8 is 480, and UTC-6 is -360

        """

        original_daily_data = pd.DataFrame()

        start_d = datetime.strptime(start, '%Y-%m-%d')
        init_end_d = end_d = datetime.strptime(end, '%Y-%m-%d')
        init_end_d.replace(hour=23, minute=59, second=59)
        delta = timedelta(days=delta)

        itr_d = end_d - delta
        overlap_start = None

        df = pd.DataFrame()
        ol = pd.DataFrame()

        while end_d > start_d:
            tf = itr_d.strftime('%Y-%m-%d') + ' ' + end_d.strftime('%Y-%m-%d')
            if self.verbose: self.logger.info('Fetching \'' + keyword + '\' for period:' + tf)
            temp = self._fetch_data(pd.Series(data=[keyword]), timeframe=tf, cat=cat, geo=geo, gprop=gprop)

            if 'isPartial' in temp.columns:
                temp.drop(columns=['isPartial'], inplace=True)

            temp.columns.values[0] = tf
            ol_temp = temp.copy()
            ol_temp.iloc[:, :] = None

            original_daily_data = pd.concat([original_daily_data, temp], axis=1)
            df = pd.concat([df, temp], axis=1)  # concatenate data
            ol = pd.concat([ol, ol_temp], axis=1)

            # shift the timeframe for next iteration
            end_d -= delta
            itr_d -= delta
            # in case of short query interval getting banned by server
            time.sleep(sleep)

        df.sort_index(inplace=True)
        ol.sort_index(inplace=True)

        # The daily trend data is missing the most recent 3-days data, need to complete with hourly data
        if df.index.max() < init_end_d:
            tf = 'now 7-d'
            hourly = self._fetch_data([keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
            hourly.drop(columns=['isPartial'], inplace=True)

            # convert hourly data to daily data
            daily = hourly.groupby(hourly.index.date).sum()

            # check whether the first day data is complete (i.e. has 24 hours)
            daily['hours'] = hourly.groupby(hourly.index.date).count()
            if daily.iloc[0].loc['hours'] != 24: daily.drop(daily.index[0], inplace=True)
            daily.drop(columns='hours', inplace=True)

            daily.set_index(pd.DatetimeIndex(daily.index), inplace=True)
            daily.columns = [tf]

            ol_temp = daily.copy()
            ol_temp.iloc[:, :] = None

            # find the overlapping date
            intersect = df.index.intersection(daily.index)
            if self.verbose:
                self.logger.info('Normalize by overlapping period:' + (intersect.min().strftime('%Y-%m-%d'))
                      + ' ' + (intersect.max().strftime('%Y-%m-%d')))

            # scaling use the overlapped today-4 to today-7 data
            coef = df.loc[intersect].iloc[:, 0].max() / daily.loc[intersect].iloc[:, 0].max()
            daily = (daily * coef).round(decimals=0)
            ol_temp.loc[intersect, :] = 1

            df = pd.concat([daily, df], axis=1)
            ol = pd.concat([ol_temp, ol], axis=1)

        # taking averages for overlapped period
        df = df.mean(axis=1)
        ol = ol.max(axis=1)
        # merge the two dataframe (trend data and overlap flag)
        df = pd.concat([df, ol], axis=1)
        df.columns = [keyword, 'overlap']
        # Correct the timezone difference
        df.index = df.index + timedelta(minutes=tz)
        df = df[start_d:init_end_d]
        # re-normalized to the overall maximum value to have max =100
        df[keyword] = (100 * df[keyword] / df[keyword].max()).round(decimals=0)

        original_daily_data = df
        return original_daily_data

