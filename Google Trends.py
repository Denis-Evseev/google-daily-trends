import pytrends
import time
from pytrends.request import TrendReq
from random import randint
import pandas as pd

pytrend = TrendReq(hl='en-US', tz=360)

df = pd.read_csv('search_df.csv')
keywords = df['keyword'];

#Set up time frames
timeframes = []
datelist = pd.date_range('2004-01-01', '2021-01-01', freq="AS")
date = datelist[0]

while date <= datelist[len(datelist)-1]:
	start_date = date.strftime("%Y-%m-%d")
	end_date = (date + pd.Timedelta(4, unit='D')).strftime("%Y-%m-%d")
	timeframes.append(start_date+' '+end_date)
	date = date + pd.Timedelta(3, unit='D')

data_all = pd.DataFrame()

for term in keywords:
	kw_list = [term]
	start_date = "2004-01-01"
	end_date = "2021-01-01"
	results = pd.DataFrame()
	count = 1
	while count < 5:
		timeframe = start_date + " " + end_date
		pytrend.build_payload(kw_list, cat=0, timeframe = timeframe)
		df=pytrend.interest_over_time()
		## df = df.drop(['isPartial', 'google'], axis=1)
		if count != 1:
			if df[term].values[0] == 0:
				scaling_factor = 1
			else:
				#Scaling factor:
				scaling_factor = float(results[term].values[-1])/float(df[term].values[0])
				#print "Scaling Factor: "+str(scaling_factor)
			df = df*scaling_factor
			results=results.append(df[1:])
		else:
			results = results.append(df)
		start_date = df.index[-1].strftime("%Y-%m-%d")
		end_date = "20" + str(int(start_date[2:4])+4) + "-" + start_date[5:]
		count = count+1
	if kw_list != [keywords[0]]:
		data_all = pd.concat([data_all, results], axis=1)


#Export as .csv file
data_all.to_csv('gtrends_results.csv')