


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

tmp_df2 = pd.DataFrame()


def plot_c_ts(dfin, cname, title):
    tmp_df = dfin[ dfin.Currency == cname]
    tmp_df = tmp_df[ tmp_df['hour'] == 15]
    tmp_df = tmp_df.sort_values(by='date')
    plt.plot(tmp_df.date, tmp_df.Close)
    plt.title(cname)
    plt.show()

def check_missing_records(tmp_df2):
    foo2 = tmp_df2[['Date', 'datetime']].copy()
    foo2 = foo2[~foo2.datetime.duplicated()].copy()
    foo2 = foo2.sort_values('datetime')
    foo2['tdelta'] = foo2.datetime - foo2.datetime.shift(1)
    foo2['tdelta2'] = foo2.datetime - foo2.datetime.shift(-1)
    print(foo2.tdelta.value_counts())
    print(foo2.tdelta2.value_counts())
    print(foo2[foo2['tdelta2'] > datetime.timedelta(hours=1)])
    print(foo2[foo2['tdelta2'] < datetime.timedelta(hours=-1)])
    print(foo2[foo2['tdelta'] < datetime.timedelta(hours=-1)])
    print(foo2[foo2['tdelta'] > datetime.timedelta(hours=1)])

check_missing_records(tmp_df2)

def check_missing_records_util(tmp_df2, target_datetime):
    foo2 = tmp_df2[['Date', 'datetime']].copy()
    foo2 = foo2[~foo2.datetime.duplicated()].copy()
    foo2 = foo2.sort_values('datetime')
    foo2['tdelta'] = foo2.datetime - foo2.datetime.shift(1)

    foo3 = foo2[ (foo2['datetime'] >= (target_datetime - datetime.timedelta(days=1))) & (foo2['datetime'] <= (target_datetime + datetime.timedelta(days=1)))]
    print(foo3)

check_missing_records_util(tmp_df2, datetime.datetime(2023, 3, 4))




plot_c_ts(tmp_df2, 'BTC-USD','foo')
plot_c_ts(tmp_df2, 'ETH-USD','foo')
plot_c_ts(tmp_df2, 'AAVE-USD','foo')
plot_c_ts(tmp_df2, 'DOGE-USD','foo')
plot_c_ts(tmp_df2, 'DAI-USD','foo')
plot_c_ts(tmp_df2, 'FLR-USD','foo')
plot_c_ts(tmp_df2, 'AVAX-USD','foo')



actual_currencies = tmp_df2['Currency'].unique().tolist()


def return_corr(dfin, curr_list):
    my_curr_list = curr_list.copy()

    btc_df = dfin[ (dfin.Currency == "BTC-USD") & (dfin.hour == 15)]
    btc_df = btc_df.sort_values(by='datetime')
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



tmp_df2['Currency'].value_counts()

def plot_vol_by_hour(dfin, cname):
    tmp_df = dfin[ dfin.Currency == cname]

    vol_by_hour = tmp_df.groupby('hour').agg({'Volume':'median','hlspread':'median'})
    vol_by_hour = vol_by_hour.reset_index()
    vol_by_hour.head()
    plt.bar(vol_by_hour.hour, vol_by_hour.Volume)
    plt.title(f"Volume by hour {cname}")
    plt.show()

    plt.plot(vol_by_hour.hour, vol_by_hour.hlspread)
    plt.title(f"Volatility by hour {cname}")
    plt.show()

plot_vol_by_hour(tmp_df2, 'BTC-USD')
plot_vol_by_hour(tmp_df2, 'ETH-USD')
plot_vol_by_hour(tmp_df2, 'SOL-USD')
plot_vol_by_hour(tmp_df2, 'DOGE-USD')
plot_vol_by_hour(tmp_df2, 'XRP-USD')
plot_vol_by_hour(tmp_df2, 'SHIB-USD')

def vol_traded_by_currency(dfin):
    tmp_df = dfin[ dfin['hour'] == 10]
    tmp_df['DollarVolume'] = tmp_df.Volume * tmp_df.Close
    vol_by_currency = tmp_df.groupby('Currency').agg({'DollarVolume':'sum',})
    vol_by_currency  =vol_by_currency.sort_values("DollarVolume")
    vol_by_currency = vol_by_currency.reset_index()
    plt.bar(vol_by_currency.index, vol_by_currency.DollarVolume)
    plt.show()
    print(vol_by_currency)
    return vol_by_currency

vol_traded_by_currency(tmp_df2)


# plimits = tmp_df2.groupby('Currency')['Close'].agg('min')
# plimits = plimits.reset_indexd()
# plimits = plimits.reset_index()
# plimits
# plimits.head()
# plimits.Close > 1.0
# (plimits.Close > 1.0).sum()
# (plimits.Close > 0.5).sum()
# (plimits.Close > 0.1).sum()

def realized_volatility(dfin):
    actual_currencies = dfin['Currency'].unique().tolist()
    for cname in actual_currencies:
        print(cname,"\t", dfin[dfin.Currency == cname]['pct_chg1'].std() * np.sqrt(365))

