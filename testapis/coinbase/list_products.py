import http.client
import json
import pickle
import requests


#'Coinbase': "https://api.exchange.coinbase.com/products/{}/candles?start={}&end={}&granularity={}",

conn = http.client.HTTPSConnection("api.exchange.coinbase.com")
payload = ''
headers = {
  'Content-Type': 'application/json'
}
conn.request("GET", "/api/v3/brokerage/products", payload, headers)
res = conn.getresponse()
data = res.read()
print(data.decode("utf-8"))

url="https://api.exchange.coinbase.com/products"
response = requests.get(url)
data = response.json()

for i in data:
  klist = list(i.keys())
  if 'quote_currency' not in klist:
    print(i)


cnt=0
for i in data:
    if (i.get("quote_currency")=='USD') & (i.get('status') != 'delisted'):
        print(f"ID: {i.get('id')}\tStatus: {i.get('status')}" )
        cnt = cnt + 1


with open('list_products.pickle', 'wb') as handle:
    pickle.dump( data, handle)

#with open('list_products.pickle', 'rb') as handle:
with open('testapis/coinbase/list_products.pickle', 'rb') as handle:
        data = pickle.load(handle)

usd_pairs = [x['id'] for x in data if ((x.get('quote_currency')== 'USD') * (x.get('status')!='disabled'))]

#stable coins
[x['id'] for x in data if ((x.get('quote_currency')== 'USD') * (x.get('status')!='disabled') & x.get('fx_stablecoin'))]
['GYEN-USD', 'BUSD-USD', 'USDT-USD', 'UST-USD', 'DAI-USD', 'PYUSD-USD', 'MUSD-USD', 'GUSD-USD', 'EURC-USD', 'PAX-USD']

margin_enabled	post_only	limit_only	cancel_only


[x['id'] for x in data if ((x.get('quote_currency')== 'USD') * (x.get('status')!='disabled') & x.get('auction_mode'))]
[]


[x['id'] for x in data if ((x.get('quote_currency')== 'USD') * (x.get('status')!='disabled') & x.get('limit_only'))]
['ZEC-USD', 'GYEN-USD', 'TIME-USD', 'GNO-USD', 'PYUSD-USD', 'GUSD-USD', 'PAX-USD', 'TAO-USD', 'INV-USD']



url="https://api.exchange.coinbase.com/product_book?product_id=BTC-USD" # does not work
url='https://api.coinbase.com/api/v3/brokerage/product_book?product_id=BTC-USD' # does not work
url='https://api.exchange.coinbase.com/products/BTC-USD/book' # works
url='https://api.exchange.coinbase.com/products/BTC-USD/book?level=2' # works
url='https://api.exchange.coinbase.com/products/FLOKI-USD/book?level=2' # works


response = requests.get(url)
data = response.json()

aa = [(float(x),float(y)) for (x,y,z) in data['bids'][0:10]]
import pandas as pd
adf = pd.DataFrame(aa, columns=['x','y'])
adf = adf.sort_values('x')
plt.plot(adf.x, adf.y)
plt.show()
adf['value'] = adf.x * adf.y / 10**5
adf


# Save a dictionary into a pickle file.


# favorite_color = { "lion": "yellow", "kitty": "red" }
# pickle.dump( favorite_color, open( "save.p", "wb" ) )
# favorite_color = pickle.load( open( "save.p", "rb" ) )
# favorite_color is now { "lion": "yellow", "kitty": "red" }


