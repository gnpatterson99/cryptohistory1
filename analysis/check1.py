


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#def load_data():

tmp_df = pd.read_csv("/Users/george/PycharmProjects/cryptohistory1/ddownload/2024.t2.year.csv")


def cleanup(input_df):
    mydf = input_df.copy()
    mydf['datetime'] = pd.to_datetime(mydf.Date)
    # mydf.head()
    # mydf.datetime.dt
    # mydf.datetime.dt.hour
    # mydf.datetime.dt.date
    # mydf['datetime'].duplicated().sum()
    # mydf['datetime'].duplicated()
    # ~mydf['datetime'].duplicated()
    mydf2 = mydf[~mydf[['datetime','Currency']].duplicated()].copy()
    mydf2['hour'] = mydf2.datetime.dt.hour
    mydf2['date'] = mydf2.datetime.dt.date
    mydf2['hlspread'] = (mydf2.High - mydf2.Low) / mydf2.Close

    return mydf2


def plot_c_ts(dfin, cname, title):
    tmp_df = dfin[ dfin.Currency == cname]
    tmp_df = tmp_df[ tmp_df['hour'] == 15]
    tmp_df = tmp_df.sort_values(by='date')
    plt.plot(tmp_df.date, tmp_df.Close)
    plt.title(cname)
    plt.show()

plot_c_ts(tmp_df2, 'BTC-USD','foo')
plot_c_ts(tmp_df2, 'ETH-USD','foo')
plot_c_ts(tmp_df2, 'AAVE-USD','foo')
plot_c_ts(tmp_df2, 'DOGE-USD','foo')
plot_c_ts(tmp_df2, 'DAI-USD','foo')
plot_c_ts(tmp_df2, 'FLR-USD','foo')


actual_currencies = tmp_df2['Currency'].unique().tolist()


def return_corr(dfin, curr_list):
    my_curr_list = curr_list.copy()

    btc_df = dfin[ (dfin.Currency == "BTC-USD") & (dfin.hour == 15)]
    btc_df = btc_df.sort_values(by='date')
    btc_df['BTC-USD'] = btc_df.Close.pct_change()
    result  = btc_df[['date', 'BTC-USD']].copy()
    my_curr_list.remove('BTC-USD')

    for cname in my_curr_list:
        btc_df = dfin[(dfin.Currency == cname) & (dfin.hour == 15)]
        btc_df = btc_df.sort_values(by='date')
        btc_df[cname] = btc_df.Close.pct_change()
        result = pd.merge(result, btc_df[['date', cname]], on='date',how='left')

    # print(result.corr(curr_list))

    return result

x = return_corr(tmp_df2, actual_currencies)
for cname in actual_currencies:
    print(cname,"\t", x[cname].std() * np.sqrt(365))



tmp_df2 = cleanup(tmp_df)
tmp_df2['Currency'].value_counts()


btc = tmp_df[ tmp_df.Currency == 'BTC-USD']
type(btc.Date[0])
btc['datetime'] = pd.to_datetime(btc.Date)
btc.head()
btc.datetime.dt
btc.datetime.dt.hour
btc.datetime.dt.date
btc['datetime'].duplicated().sum()
btc['datetime'].duplicated()
~btc['datetime'].duplicated()


btc2 = btc[~btc['datetime'].duplicated()].copy()


btc2['hour'] = btc2.datetime.dt.hour
btc2['hlspread'] = (btc2.High - btc2.Low) / btc2.Close


vol_by_hour = btc2.groupby('hour').agg({'Volume':'median','hlspread':'median'})
vol_by_hour = vol_by_hour.reset_index()
vol_by_hour.head()
plt.bar(vol_by_hour.hour, vol_by_hour.Volume)
plt.show()

plt.plot(vol_by_hour.hour, vol_by_hour.hlspread)
plt.show()



tmp_df2.groupby('Currency').agg({'datetime':'min'})

print("hello world")
