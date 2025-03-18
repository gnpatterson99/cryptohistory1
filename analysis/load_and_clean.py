import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime

#def load_data():

file_list = ['2024.t2.year.csv','2023.q4.csv','2023.q3.csv','2023.q2.csv','2023.q1.csv', '2022.year.csv','2022b.year.csv','backfill.1.csv','backfill.2.csv']
file_list = ['Coinbase_86400_2000-12-31T00_00_00_2024-12-31T23_00_00']
in_df = None
for file in file_list:
    tmp_in_df = pd.read_csv("/Users/george/PycharmProjects/cryptohistory1/ddownload/"+ file)
    if in_df is None:
        in_df = tmp_in_df.copy()
    else:
        in_df = pd.concat([in_df, tmp_in_df],ignore_index=True)


def cleanup(input_df):
    mydf = input_df.copy()
    mydf['datetime'] = pd.to_datetime(mydf.Date)
    mydf2 = mydf[~mydf[['datetime','Currency']].duplicated()].copy()
    mydf2['year'] = mydf2.datetime.dt.year
    mydf2['month'] = mydf2.datetime.dt.month
    mydf2['qtr'] = mydf2.datetime.dt.quarter
    mydf2['hour'] = mydf2.datetime.dt.hour
    mydf2['date'] = mydf2.datetime.dt.date
#    mydf2['hlspread'] = (mydf2.High - mydf2.Low) / mydf2.Close
    return mydf2

tmp_df2 = cleanup(in_df)

def check_missing_records(tmp_df2, hours=1):
    foo2 = tmp_df2[['Date', 'datetime']].copy()
    foo2 = foo2[~foo2.datetime.duplicated()].copy()
    foo2 = foo2.sort_values('datetime')
    foo2['tdelta'] = foo2.datetime - foo2.datetime.shift(1)
    foo2['tdelta2'] = foo2.datetime - foo2.datetime.shift(-1)
    print(foo2.tdelta.value_counts())
    print(foo2.tdelta2.value_counts())
    print(foo2[foo2['tdelta2'] > datetime.timedelta(hours=hours)])
    print(foo2[foo2['tdelta2'] < datetime.timedelta(hours=-1*hours)])
    print(foo2[foo2['tdelta'] < datetime.timedelta(hours=-1*hours)])
    print(foo2[foo2['tdelta'] > datetime.timedelta(hours=hours)])

check_missing_records(tmp_df2, hours=24)

def check_missing_records_util(tmp_df2, target_datetime):
    foo2 = tmp_df2[['Date', 'datetime']].copy()
    foo2 = foo2[~foo2.datetime.duplicated()].copy()
    foo2 = foo2.sort_values('datetime')
    foo2['tdelta'] = foo2.datetime - foo2.datetime.shift(1)

    foo3 = foo2[ (foo2['datetime'] >= (target_datetime - datetime.timedelta(days=1))) & (foo2['datetime'] <= (target_datetime + datetime.timedelta(days=1)))]
    print(foo3)

# check_missing_records_util(tmp_df2, datetime.datetime(2023, 3, 4))

def currencies_no_trading(dfin, target_hour=10):
    # tmp_df = dfin[ dfin['hour'] == target_hour].copy()
    tmp_df = dfin.copy()
    min_date_by_currency = tmp_df.groupby('Currency').agg({'datetime':'min'})
    max_date_by_currency = tmp_df.groupby('Currency').agg({'datetime':'max'})

    date_list = tmp_df.datetime.unique().tolist()

    for mycurr in min_date_by_currency.index:
        my_min_date = min_date_by_currency.loc[mycurr,'datetime']
        my_max_date = max_date_by_currency.loc[mycurr,'datetime']

        date_list2 = [i for i in date_list if i >= my_min_date]
        date_list2 = [i for i in date_list2 if i <= my_max_date]

        if len(date_list2) == 0:
            print("No dates in datelist2")
            raise ValueError


        seen_dates = tmp_df[tmp_df.Currency == mycurr].datetime.unique().tolist()
        missing_dates = set(date_list2) - set(seen_dates)
        missing_dates = list(missing_dates)
        missing_dates.sort()
        if len(missing_dates) > 0:
            print(f"Missing dates {mycurr}, First Record: {my_min_date}, Last Record: {my_max_date}")
            # for my_missing in missing_dates:
            #     print(f"{my_missing}")
            print("Number of missing dates: ", len(missing_dates))
            print("Avg Price ", tmp_df[tmp_df.Currency == mycurr]['Close'].mean())
            print("\n\n")

currencies_no_trading(tmp_df2)

# def plot_c_ts(dfin, cname, title):
#     tmp_df = dfin[ dfin.Currency == cname]
#     tmp_df = tmp_df[ tmp_df['hour'] == 15]
#     tmp_df = tmp_df.sort_values(by='date')
#     plt.plot(tmp_df.date, tmp_df.Close)
#     plt.title(cname)
#     plt.show()
