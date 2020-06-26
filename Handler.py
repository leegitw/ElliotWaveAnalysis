import pandas as pd
from Swings import *
from ElliotAnalyzer import *
import os
import time
import configparser
import shutil
from tiingo import TiingoClient
import pandas as pd
import numpy

DATA_PATH = "./ForexData"
GRAPHS_PATH = "./ForexGraphs"
ANALYSIS_SUMMARY_FILE = "summary_analysis.txt"
CONFIG_FILE= "Handler_Config.conf"
PAIRS_FILE = "Pair_Analysis.txt"
TYPICAL = False

startDate = "2020-01-01"
endDate = "2020-06-20"

#TODO: add support for other config options to do big multiconfig, multitimeframe analysis at once
########################################################################################################################

def config_section_map(config, section):
    dict1 = {}
    options = config.options(section)
    for option in options:
        try:
            dict1[option] = config.get(section, option)
            if dict1[option] == -1:
                print("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1

#Read in config
config = configparser.ConfigParser()
config.read(CONFIG_FILE)
ST_config = config_section_map(config, "Short_Term")
IT_config = config_section_map(config, "Intermediate_Term")
LT_config = config_section_map(config, "Long_Term")
MT_config = config_section_map(config, "Major_Term")
config_list = [(ST_config, "ST"), (IT_config, "IT"), (LT_config, "LT"), (MT_config, "MT")] #TODO: determine which configs to use this time

#Set up analysis parameters
typical = config_section_map(config, "Other")["typical"]

########################################################################################################################

#Get the pairs to analyze
with open(PAIRS_FILE, 'r') as infile:
    pairs_to_analyze = infile.read().splitlines()

outfile = open(ANALYSIS_SUMMARY_FILE, 'w')

#Clear old graphs
for file in os.listdir(GRAPHS_PATH):
    os.remove(os.path.join(GRAPHS_PATH, file))

tgo = TiingoClient()

from polygon import RESTClient
pgo = RESTClient(os.getenv("POLYGON_API_KEY") )

import psycopg2
dbconn = psycopg2.connect(host="127.0.0.1", port=8433,database="shared", user="postgres", password="postgres")


def quotes_minute(symbol, start_date, end_date):
    return quotes(symbol, start_date, end_date, 'quotes_minute')


def quotes(symbol, start_date, end_date,  db_table):
    symbols = []
    dates = []
    open = []
    high = []
    low = []
    close = []
    volume = []
    updated_at = []

    #print("%s %s %s %s " % (db_table, symbol, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

    #
    q = "SELECT " \
        "date AT TIME ZONE 'America/New_York' as date, " \
        "adj_open as open," \
        "adj_high as high," \
        "adj_low as low," \
        "adj_close as close," \
        "adj_volume as volume, " \
        "updated_at " \
        "FROM %s " \
        "WHERE market_closed = false AND symbol = '%s' AND date >= '%s 00:00:00' AND date <= '%s 23:59:59' " \
        "ORDER BY date" % (db_table, symbol, start_date, end_date)
    print(q)

    try:
        cur = dbconn.cursor()
        cur.execute(q)

        rows = cur.fetchall()

        for row in rows:

            dh = int(row[0].strftime('%H'))
            dm = int(row[0].strftime('%M'))

            if dh < 9 or (dh == 9 and dm < 30) or dh >= 16 :
                continue

            symbols.append(symbol)
            dates.append(row[0])
            open.append( row[1])
            high.append(  row[2])
            low.append( row[3])
            close.append( row[4])
            volume.append( row[5])
            updated_at.append( row[6])

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    return {
        'symbol': symbols,
        'period_date': dates,
        'open': numpy.array(open, dtype=float),
        'high': numpy.array(high, dtype=float),
        'low': numpy.array(low, dtype=float),
        'close': numpy.array(close, dtype=float),
        'volume': numpy.array(volume, dtype=float),
        'updated_at': updated_at
    }


def concat_period_dates(list):
    l = []
    for v in list :
        if isinstance(v, str) == False :
            v = v.strftime('%Y-%m-%dT%H:%M')
        l.append(v)

    return ','.join(l)


########################################################################################################################
#Begin analyzing Pairs
for pair in pairs_to_analyze:
    pairname, pairtime = pair.split("_")

    pts = pair.split("_")
    ticker = pts[0]

    freq = pts[1]
    if len(freq) == 1 :
        freq = freq+"1"





    if False :
        if freq == "H" or freq.startswith("H") :
            freq = freq.replace("H", "")+"Hour"
        elif freq == "D" or  freq.startswith("D") :
            freq = freq.replace("D", "")+"Day"
        elif freq == "M" or  freq.startswith("M") :
            freq = freq.replace("M", "")+"Min"

        pu =  "/tiingo/fx/%s/prices?resampleFreq=%s&startDate=%s&endDate=%s" % (ticker, freq, startDate, endDate)
        pu =  "/tiingo/daily/%s/prices?startDate=%s&endDate=%s&resampleFreq=daily" % (ticker,  startDate, endDate)
        response = tgo._request('GET', pu)
        df = pd.DataFrame(response.json())

        df = df.rename(columns={
            'date': 'Date_Time',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
        })
    elif False :
        pu = "https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/week/2019-01-01/2019-02-01?sort=asc&apiKey=rmr_K5BO36nfgC_N6qpPNt95kMY_Z3F_iX_0aC"

        if freq == "H" or freq.startswith("H") :
            timespan = "hour"
            multiplier = int(freq.replace("H", ""))
        elif freq == "D" or  freq.startswith("D") :
            timespan = "day"
            multiplier = int(freq.replace("D", ""))
        elif freq == "M" or  freq.startswith("M") :
            timespan = "minute"
            multiplier = int(freq.replace("M", ""))
        res = pgo.stocks_equities_aggregates(ticker, multiplier, timespan, startDate, endDate)

        df = pd.DataFrame(res.results)
        df = df.rename(columns={
            't': 'Date_Time',
            'o': 'Open',
            'h': 'High',
            'l': 'Low',
            'c': 'Close',
        })
        df['Date_Time'] = df['Date_Time'].map(lambda n: int(n/1000))
        df['Date_Time'] = pd.to_datetime(df['Date_Time'], unit="s")
    else :
        if freq == "H" or freq.startswith("H") :
            p = freq.replace("H", "")+"h"
        elif freq == "D" or  freq.startswith("D") :
            p = freq.replace("D", "")+"d"
        elif freq == "M" or  freq.startswith("M") :
            p = freq.replace("M", "")+"min"

        ohlc_dict = {'period_date': 'first', 'symbol':'first', 'open':'first','high':'max','low':'min','close': 'last','volume': 'sum', 'updated_at': max}
        min1 = quotes_minute(ticker, startDate, endDate)

        raw_df = pd.DataFrame(min1)
        raw_df.index = pd.to_datetime(raw_df.period_date)


        df = raw_df.resample(p, closed='left', label='left').apply(ohlc_dict).dropna()
        df = df.rename(columns={
            'period_date': 'Date_Time',
            'open': 'Open',
            'high': 'High',
            'low': 'Low',
            'close': 'Close',
        })


    print(df.head())

    for current_config, config_name in config_list:
        forex_swing_file =  pair + "_swings_" + current_config["atr_period"] + "_" + current_config["time_factor"] + "_" + current_config["price_factor"] + ".csv"

        sg = Swing_Generator(df,forex_swing_file, current_config)
        if(os.path.isfile(forex_swing_file)):
            sg.update_swings()
        else:
            sg.generate_swings()

        ea = Elliot_Analyzer(pair, forex_swing_file, df)
        analysis_summary = ea.analyze()
        print("analysis_summary", analysis_summary)

        for result in analysis_summary:
            print(ea.wave_data[result])

            if typical == "1":
                if ea.wave_data[result][1] != "Minimum":
                    ea.export_graphs(os.path.join(GRAPHS_PATH, pair + "_" + config_name))
                    outfile.write(pair + "\t" + result + "\t" + ea.wave_data[result][1] + "\t" + config_name + "\n")
                else :
                    print("skip chart min")
            else:
                ea.export_graphs(os.path.join(GRAPHS_PATH,  pair + "_" + config_name))
                outfile.write(pair + "\t" + result + "\t" + ea.wave_data[result][1] + "\t" + config_name + "\n")

        print('.', end='', flush=True)

outfile.close()

print("Total Time: ", time.process_time())
