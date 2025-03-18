
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

# from analysis.check1 import actual_currencies
# from analysis.trailingstats import tmp_df2


# hourly reversal

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


def cleanlag(dfin, v1, v2, lags):
    f = dfin[v1] != dfin[v1].shift(lags)

#    btc_df.loc[f,'pct_chg'] = pd.NA

    f2 = (dfin.datetime - dfin.datetime.shift(lags))
    f2 = pd.to_timedelta(f2)
    f2 = (f2 != datetime.timedelta(hours=lags))

    return f & f2


def generate_pctcht(dfin, hour_multiplier=1, maxlag=6, btc_adjust=False):

    p_df = dfin.sort_values(by=['Currency','datetime'])
    p_df['pct_chg1'] = p_df.Close.pct_change(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',1*hour_multiplier),'pct_chg1'] = pd.NA

    # btc_df = p_df[p_df.Currency == "BTC-USD"][['datetime','pct_chg1']].copy()
    # btc_df.rename(columns={'pct_chg1':'pct_chg1_btc'}, inplace=True)
    # p_df = pd.merge(p_df, btc_df, on='datetime', how='left')
    # p_df['pct_chg1'] = p_df['pct_chg1'] - p_df['pct_chg1_btc']

    for i in range(2,maxlag):
        p_df['pct_chg'+str(i)] = p_df['pct_chg'+str(i-1)].shift(1)
        p_df.loc[cleanlag(p_df, 'Currency', 'datetime', 2 * hour_multiplier), 'pct_chg'+str(i)] = pd.NA

    # p_df['pct_chg2'] = p_df['pct_chg1'].shift(1)
    # p_df.loc[cleanlag(p_df,'Currency','datetime',2*hour_multiplier),'pct_chg2'] = pd.NA
    #
    # p_df['pct_chg3'] = p_df['pct_chg2'].shift(1)
    # p_df.loc[cleanlag(p_df,'Currency','datetime',3*hour_multiplier),'pct_chg3'] = pd.NA
    #
    # p_df['pct_chg4'] = p_df['pct_chg3'].shift(1)
    # p_df.loc[cleanlag(p_df,'Currency','datetime',4*hour_multiplier),'pct_chg4'] = pd.NA
    #
    # p_df['pct_chg5'] = p_df['pct_chg4'].shift(1)
    # p_df.loc[cleanlag(p_df,'Currency','datetime',5*hour_multiplier),'pct_chg5'] = pd.NA
    #
    # p_df['pct_chg6'] = p_df['pct_chg5'].shift(1)
    # p_df.loc[cleanlag(p_df,'Currency','datetime',6*hour_multiplier),'pct_chg6'] = pd.NA

    return p_df


def restack(dfin):

    my_curr_list = list(dfin.Currency.unique())

    btc_df = dfin[ (dfin.Currency == "BTC-USD")][['datetime', 'pct_chg1']]
    btc_df.rename(columns={'pct_chg1':'BTC-USD'}, inplace=True)
    result  = btc_df[['datetime', 'BTC-USD']].copy()
    my_curr_list.remove('BTC-USD')

    for cname in my_curr_list:
        btc_df = dfin[(dfin.Currency == cname)][['datetime', 'pct_chg1']]
        btc_df.rename(columns={'pct_chg1': cname}, inplace=True)

        result = pd.merge(result, btc_df[['datetime', cname]], on='datetime',how='left')
    return result


# tmp_df3 = generate_pctcht(tmp_df2)
# tmp_df4 = restack(tmp_df3)
# actual_currencies = tmp_df2['Currency'].unique().tolist()
#
# aa = tmp_df4[actual_currencies].isna().sum().sort_values()
# #long_hist_currencies = list(aa.index[0:17])
# long_hist_currencies = list(aa.index[0:16])
#
# tmp_df5 = tmp_df4[long_hist_currencies].copy()
# tmp_df5 = tmp_df5.fillna(0.0)
#


def reversal(dfin, curr_list):

    my_curr_list = curr_list.copy()

    p_df = dfin.sort_values(by=['Currency','datetime'])
    p_df['pct_chg1'] = p_df.Close.pct_change(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',1),'pct_chg1'] = pd.NA

    btc_df = p_df[p_df.Currency == "BTC-USD"][['datetime','pct_chg1']].copy()
    btc_df.rename(columns={'pct_chg1':'pct_chg1_btc'}, inplace=True)
    p_df = pd.merge(p_df, btc_df, on='datetime', how='left')
    p_df['pct_chg1'] = p_df['pct_chg1'] - p_df['pct_chg1_btc']

    p_df['pct_chg2'] = p_df['pct_chg1'].shift(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',2),'pct_chg2'] = pd.NA

    p_df['pct_chg3'] = p_df['pct_chg2'].shift(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',3),'pct_chg3'] = pd.NA

    p_df['pct_chg4'] = p_df['pct_chg3'].shift(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',4),'pct_chg4'] = pd.NA

    p_df['pct_chg5'] = p_df['pct_chg4'].shift(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',5),'pct_chg5'] = pd.NA

    p_df['pct_chg6'] = p_df['pct_chg5'].shift(1)
    p_df.loc[cleanlag(p_df,'Currency','datetime',6),'pct_chg6'] = pd.NA


    p_df[['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4','pct_chg5','pct_chg6']].corr()
    p_df[p_df.Currency != 'BTC-USD'][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()
    p_df[p_df.Currency == 'BTC-USD'][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()

    p_df[p_df.Close > 1.0][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()

    p_df[p_df.hour < 9 ][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()
    p_df[p_df.hour > 16][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()
    p_df[(p_df.hour > 8) & (p_df.hour < 16) ][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()
    p_df[(p_df.hour < 9) | (p_df.hour > 16) ][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4']].corr()

    # plt.hist(p_df['pct_chg1'], bins=50)
    # p_df[p_df.pct_chg1 > 0.2]
    # p_df[p_df.pct_chg1 > 0.2].columns
    # outlier_high = p_df[p_df.pct_chg1 > 0.1]
    # outlier_high[['Currency', 'datetime']]
    # outlier_high[['Currency', 'datetime', 'pct_chg1', 'Open', 'Close']]
    #
    # outlier_high['oc_return'] = (outlier_high['Close'] - outlier_high['Open']) / outlier_high['Open']
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_colwidth', 100)
    # pd.set_option('display.width', 100)
    # outlier_high[['Currency', 'datetime', 'pct_chg1', 'Open', 'Close', 'oc_return']]
    # plt.plot(outlier_high[['pct_chg1', 'oc_return']])
    # plt.plot(outlier_high['pct_chg1'], outlier_high['oc_return'])
    # plt.scatter(outlier_high['pct_chg1'], outlier_high['oc_return'])
    # plt.show()
    # outlier_high.sort_values('pct_chg1')
    # outlier_high.sort_values('pct_chg1')[['Currency', 'datetime', 'pct_chg1', 'Close']]
    # plot_c_ts(tmp_df2, 'ENS-USD')
    # plot_c_ts(tmp_df2, 'ENS-USD', 'ENS')
