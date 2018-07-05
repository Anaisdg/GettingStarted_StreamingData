import pandas as pd
import requests
import time
from alphavantage_auth import key
import datetime

#Using Alphavantage to get BTC prices every 5 make_lines
#Get your key here: https://www.alphavantage.co/support/#api-key

apikey = key
url = "https://www.alphavantage.co/query?"
function = "DIGITAL_CURRENCY_INTRADAY"
symbol = "BTC"
market = "USD"

#build target url
target_url = url + "function=" + function + "&symbol=" + symbol + "&market=" + market + "&apikey=" + apikey

while True:
    #make request
    data = requests.get(target_url).json()
    #data is returned in the following format: https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_INTRADAY&symbol=BTC&market=EUR&apikey=demo
    #we only want the last datapoint
    t = [t for t in data['Time Series (Digital Currency Intraday)']]
    t = t[0]
    t = datetime.datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
    unix = int(t.strftime("%s"))
    #convert to nanosecond precison
    unix_ns = str(unix) + "000000000"
    fields = [v for k, v in data['Time Series (Digital Currency Intraday)'].items()]
    #convert to line protocol
    line = ["price"
         + ",type=BTC"
         + " "
         + "price=" + str(fields[0]['1a. price (USD)']) + ","
         + "volume=" + str(fields[0]['2. volume'])
         + " " + unix_ns]
    thefile = open('Data/tail.txt', 'a+')
    for item in line:
        thefile.write("%s\n" % item)
    print("line added")
    #alphavantage only adds points every 5 min, so set script to sleep for 5 min as well
    time.sleep(300)
