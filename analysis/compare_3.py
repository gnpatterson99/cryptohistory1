import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

#def load_data():


def read_dat(fname, prefix):
    tmp_df = pd.read_csv("/Users/george/PycharmProjects/cryptohistory1/ddownload/"+fname)
    tmp_df['datetime'] = pd.to_datetime(tmp_df.Date)
    tmp_df.rename(columns={'Currency':'currency'},inplace=True)
    tmp_df.rename(columns={'Price': prefix+'_price'}, inplace=True)
    tmp_df.rename(columns={'Volume': prefix+'_volume'}, inplace=True)
    tmp_df.drop(columns='Date', inplace=True)
    return tmp_df

k_dat = read_dat("Kraken_60_2025-02-10T00_00_00_2025-02-20T00_00_00","k")
c_dat = read_dat("Coinbase_3600_2025-02-10T00_00_00_2025-02-20T00_00_00","c")
b_dat = read_dat("Binance_1h_2025-02-10T00_00_00_2025-02-20T00_00_00","b")

all_dat = pd.merge(b_dat, c_dat, on=['currency','datetime'], how='outer')
all_dat = pd.merge(all_dat, k_dat, on=['currency','datetime'], how='left')

fmt = "%Y-%m-%d %H:%M:%S"

min_date = datetime.datetime.strptime("2025-02-10 15:00:00", fmt)
max_date = datetime.datetime.strptime("2025-02-19 14:00:00", fmt)

all_dat = all_dat[(all_dat['datetime'] >= min_date) & (all_dat['datetime'] <= max_date)]

def cleanup(input_df):
    mydf = input_df.copy()
    mydf['datetime'] = pd.to_datetime(mydf.Date)
    mydf2 = mydf[~mydf[['datetime','Currency']].duplicated()].copy()
    mydf2['hour'] = mydf2.datetime.dt.hour
    mydf2['date'] = mydf2.datetime.dt.date
    mydf2['hlspread'] = (mydf2.High - mydf2.Low) / mydf2.Close
    return mydf2


def plot_c_ts(dfin, cname, title='default', vlist=['k_price', 'c_price', 'b_price']):
    tmp_df = dfin[ dfin.currency == cname]
#    tmp_df = tmp_df[ tmp_df['hour'] == 15]
    tmp_df = tmp_df.sort_values(by='datetime')
    for c in vlist:
        plt.plot(tmp_df.datetime, tmp_df[c], label=c)
    plt.legend()
    plt.title(cname)
    plt.show()

btc_dat = all_dat[ all_dat.currency == 'BTC-USD']
btc_dat = btc_dat.sort_values('datetime')


#plot_c_ts(btc_dat, 'BTC-USD','BTC')
#plot_c_ts(btc_dat, 'BTC-USD','BTC')

plot_c_ts(all_dat, 'BTC-USD','BTC compare')

plot_c_ts(all_dat, 'ETH-USD','ETH compare')
plot_c_ts(all_dat, 'AAVE-USD','AAVE compare')
plot_c_ts(all_dat, 'DOGE-USD','foo')
plot_c_ts(all_dat, 'DAI-USD','foo')
plot_c_ts(all_dat, 'FLR-USD','foo')

plot_c_ts(btc_dat, 'BTC-USD','BTC',vlist=['k_volume','b_volume','c_volume'])
plot_c_ts(all_dat, 'ETH-USD','ETH',vlist=['k_volume','b_volume','c_volume'])
plot_c_ts(all_dat, 'DOGE-USD','ETH',vlist=['k_volume','b_volume','c_volume'])
plot_c_ts(all_dat, 'AAVE-USD','ETH',vlist=['k_volume','b_volume','c_volume'])
plot_c_ts(all_dat, 'FLR-USD','ETH',vlist=['k_volume','b_volume','c_volume'])


all_currencies = all_dat.currency.unique().tolist()

for ac in all_currencies:
    print(ac)
    ac2 = all_dat[all_dat.currency == 'DOGE-USD'][[ 'c_price', 'b_price', 'k_price','c_volume','b_volume','k_volume']].isna().sum()
    print(ac2)


all_dat['pmax'] = all_dat[[ 'c_price', 'b_price', 'k_price']].max(axis=1)
all_dat['pmin'] = all_dat[[ 'c_price', 'b_price', 'k_price']].min(axis=1)
all_dat['mmspread'] = 2.0 * (all_dat.pmax - all_dat.pmin) / (all_dat.pmax + all_dat.pmin)