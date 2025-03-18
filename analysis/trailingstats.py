"""
file to generate trailing stats.
"""


def trailing_std(in_df):

    tmp_df2 = in_df.sort_values(['Currency','datetime'])
    b=tmp_df2[['Currency','datetime','pct_chg6']].groupby(['Currency']).rolling('22d', on='datetime').std()
    b = b.reset_index()
    b = b.sort_values(['Currency','datetime'])
    b = b[['Currency','datetime','pct_chg6']]
    b.rename(columns={'pct_chg6':'std_pct'}, inplace=True)
    return b

