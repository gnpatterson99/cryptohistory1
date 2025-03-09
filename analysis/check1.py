


import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

#def load_data():

tmp_df = pd.read_csv("/Users/george/PycharmProjects/cryptohistory1/ddownload/2024.t2.year.csv")


def cleanup(input_df):
    mydf = input_df.copy()
    mydf['datetime'] = pd.to_datetime(mydf.Date)
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

tmp_df2 = cleanup(tmp_df)

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

def vol_traded_by_currency(dfin):
    tmp_df = dfin[ dfin['hour'] == 10]
    tmp_df['DollarVolume'] = tmp_df.Volume * tmp_df.Close
    vol_by_currency = tmp_df.groupby('Currency').agg({'DollarVolume':'sum',})
    vol_by_currency  =vol_by_currency.sort_values("DollarVolume")
    vol_by_currency = vol_by_currency.reset_index()
    plt.bar(vol_by_currency.index, vol_by_currency.DollarVolume)
    plt.show()
    return vol_by_currency

vol_traded_by_currency(tmp_df2)

def currencies_no_trading(dfin, target_hour=10):
    tmp_df = dfin[ dfin['hour'] == target_hour].copy()
    min_date_by_currency = tmp_df.groupby('Currency').agg({'datetime':'min'})
    date_list = tmp_df.datetime.unique().tolist()

    for mycurr in min_date_by_currency.index:
        my_min_date = min_date_by_currency.loc[mycurr,'datetime']
        date_list2 = [i for i in date_list if i >= my_min_date]
        if len(date_list2) == 0:
            print("No dates in datelist2")
            continue


        seen_dates = tmp_df[tmp_df.Currency == mycurr].datetime.unique().tolist()
        missing_dates = set(date_list2) - set(seen_dates)
        missing_dates = list(missing_dates)
        missing_dates.sort()
        if len(missing_dates) > 0:
            print(f"Missing dates {mycurr} at hour {target_hour} First Record: {my_min_date}")
            for my_missing in missing_dates:
                print(f"{my_missing}")
               # print(tmp_df[(tmp_df.Currency == mycurr) & (tmp_df.datetime == my_missing)])
            print("\n\n")

currencies_no_trading(tmp_df2,target_hour=20)

plimits = tmp_df2.groupby('Currency')['Close'].agg('min')
plimits = plimits.reset_indexd()
plimits = plimits.reset_index()
plimits
plimits.head()
plimits.Close > 1.0
(plimits.Close > 1.0).sum()
(plimits.Close > 0.5).sum()
(plimits.Close > 0.1).sum()