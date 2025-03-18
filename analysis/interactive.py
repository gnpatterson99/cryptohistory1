#from analysis.load_and_clean import tmp_df2
from analysis.reversal import generate_pctcht
from analysis.trailingstats import trailing_std

tmp_df2.rename(columns={'Price':'Close'}, inplace=True)
tmp_df3 = generate_pctcht(tmp_df2, hour_multiplier=24,maxlag=30)

tmp_df3_stat = trailing_std(tmp_df3)
tmp_df4 = pd.merge(tmp_df3, tmp_df3_stat, on =['Currency','datetime'], how='left')

tmp_df4['pct_chg1_std'] = tmp_df4.pct_chg1 / tmp_df4.std_pct
tmp_df4['pct_chg2_std'] = tmp_df4.pct_chg2 / tmp_df4.std_pct
tmp_df4['pct_chg3_std'] = tmp_df4.pct_chg3 / tmp_df4.std_pct
tmp_df4['pct_chg4_std'] = tmp_df4.pct_chg4 / tmp_df4.std_pct
tmp_df4['pct_chg5_std'] = tmp_df4.pct_chg5 / tmp_df4.std_pct
tmp_df4['pct_chg6_std'] = tmp_df4.pct_chg6 / tmp_df4.std_pct

plt.hist(tmp_df4.pct_chg1_std[ abs(tmp_df4.pct_chg1_std) <10.0], bins=100)
plt.show()

tmp_df4[['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4','pct_chg5','pct_chg6','pct_chg7']].corr()
tmp_df4['oflag'] = (abs(tmp_df4.pct_chg1_std) < 5) & (abs(tmp_df4.pct_chg2_std) < 5) & (abs(tmp_df4.pct_chg3_std) < 5) & (abs(tmp_df4.pct_chg4_std) < 5)
tmp_df4[abs(tmp_df4.pct_chg1_std) < 5][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4','pct_chg5','pct_chg6']].corr()
plt.hist(tmp_df4.pct_chg1_std[ abs(tmp_df4.pct_chg1_std) <10.0], bins=100)

tmp_df4[tmp_df4.oflag == True][['pct_chg1', 'pct_chg2', 'pct_chg3', 'pct_chg4','pct_chg5','pct_chg6']].corr()

a = tmp_df4.groupby('year')[['pct_chg'+str(i) for i in range(1,30)]].corr()
a['lag']=a['level_1'].apply(lambda x: int(x[7:9]))
a['lag']=a['level_1'].apply(lambda x: int(x[7:9]))
a = a[a.lag != 1].copy()

b2023 = a[ a.year==2023]
b2019 = a[ a.year==2019]
b2024 = a[ a.year==2024]
plt.plot(b2024.lag, b2024.pct_chg1,label='2024')
plt.plot(b2019.lag, b2019.pct_chg1,label='2019')
plt.plot(b2023.lag, b2023.pct_chg1,label='2015')
plt.legend()
plt.show()

from sklearn import linear_model
reg = linear_model.LinearRegression()
reg = linear_model.LinearRegression()
reg.fit( tmp_df4[ tmp_df4.oflag == True ][['pct_chg2', 'pct_chg3', 'pct_chg4']], tmp_df4['pct_chg1'])

tmp_df4_reg = tmp_df4[tmp_df4.oflag == True]
tmp_df4_reg[['pct_chg1','pct_chg2', 'pct_chg3', 'pct_chg4']].fillna(0, inplace=True)
reg.fit( tmp_df4[ tmp_df4.oflag == True ][['pct_chg2', 'pct_chg3', 'pct_chg4']], tmp_df4['pct_chg1'])
foo = reg.fit( tmp_df4_reg[['pct_chg2', 'pct_chg3', 'pct_chg4']], tmp_df4_reg['pct_chg1'])


tmp_df4[ abs(tmp_df4['pct_chg2_std'] > 6)]['pct_chg1'].describe()

large_pos_df = tmp_df4[tmp_df4.pct_chg1 > 0.20][['Currency','Open','Close','Volume','datetime']]
large_neg_df = tmp_df4[tmp_df4.pct_chg1 < -0.20][['Currency','Open','Close','Volume','datetime']]

a =tmp_df4.groupby('Currency')['pct_chg1'].std().reset_index()
