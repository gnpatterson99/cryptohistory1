import csv
import logging
import math
from datetime import datetime, timedelta

import requests

# Logging-Konfiguration
logging.basicConfig(filename='crypto_data.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Program started")
print("Program started")
start_date = "2025-02-10T00:00:00"
end_date = "2025-02-20T00:00:00"
# start_date = input("Set start date(Format:2024-02-20T12:00:00)=")
# end_date = input("Set end date(Format:2024-03-20T12:00:00=")

fmt = "%Y-%m-%dT%H:%M:%S"

start = datetime.strptime(start_date, fmt)
end = datetime.strptime(end_date, fmt)

while start > end:
    print("Start date is after end date. Pls enter a start date before the end date. ")
    start_date = input("Set start date(Format:2024-02-20T12:00:00)=")
    end_date = input("Set end date(Format:2024-03-20T12:00:00=")

kraken_interval = input("Set interval kraken[1, 5, 15, 30, 60, 240, 1440, 10080, 21600](in minutes)")
binance_interval = input("Set interval binance[1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M]")
coinbase_interval = input("Set interval coinbase[60(1m), 300(5m), 900(15m), 3600(1h), 21600(6h), 86400(1d)]")

api_endpoints = {

    'Coinbase': "https://api.exchange.coinbase.com/products/{}/candles?start={}&end={}&granularity={}",
    'Kraken': "https://api.kraken.com/0/public/OHLC?pair={}&since={}&interval={}",
    'Binance': "https://api.binance.us/api/v3/klines?symbol={}&startTime={}&endTime={}&interval={}"

}

binance_interval_map = {"1m": 60, "3m": 180, "5m": 300, "15m": 900, "30m": 1800, "1h": 3600, "2h": 7200, "4h": 14400,
                        "6h": 21600, "8h": 28800, "12h": 43200, "1d": 86400, "3d": 259200, "1w": 604800, "1M": 2592000}

excahnge_to_interval = {"Coinbase": coinbase_interval,
                        "Kraken": kraken_interval,
                        "Binance": binance_interval}

excahnge_to_interval_in_seconds = {"Coinbase": int(coinbase_interval),
                                   "Kraken": int(kraken_interval) * 60,
                                   "Binance": binance_interval_map[binance_interval]}

API_SYMBOL_MAPPIN = {'BTC-USD': {'Binance': 'BTCUSDT', 'Coinbase': 'BTC-USD', 'Kraken': 'XXBTZUSD'},
                     'ETH-USD': {'Binance': 'ETHUSDT', 'Coinbase': 'ETH-USD', 'Kraken': 'XETHZUSD'},
                     'XRP-USD': {'Binance': 'XRPUSDT', 'Coinbase': 'XRP-USD', 'Kraken': 'XXRPZUSD'},
                     'SOL-USD': {'Binance': 'SOLUSDT', 'Coinbase': 'SOL-USD', 'Kraken': 'SOLUSD'},
                     'DOGE-USD': {'Binance': 'DOGEUSDT', 'Coinbase': 'DOGE-USD', 'Kraken': 'XDGUSD'},
                     'ADA-USD': {'Binance': 'ADAUSDT', 'Coinbase': 'ADA-USD', 'Kraken': 'ADAUSD'},
                     'LINK-USD': {'Binance': 'LINKUSDT', 'Coinbase': 'LINK-USD', 'Kraken': 'LINKUSD'},
                     'XLM-USD': {'Binance': 'XLMUSDT', 'Coinbase': 'XLM-USD', 'Kraken': 'XXLMZUSD'},
                     'LTC-USD': {'Binance': 'LTCUSDT', 'Coinbase': 'LTC-USD', 'Kraken': 'XLTCZUSD'},
                     'SUI-USD': {'Binance': 'SUIUSDT', 'Coinbase': 'SUI-USD', "Kraken": "SUIUSD"},
                     'AVAX-USD': {'Binance': 'AVAXUSDT', 'Coinbase': 'AVAX-USD', 'Kraken': 'AVAXUSD'},
                     'SHIB-USD': {'Binance': 'SHIBUSDT', 'Coinbase': 'SHIB-USD', "Kraken": "SHIBUSD"},
                     'HBAR-USD': {'Binance': 'HBARUSDT', 'Coinbase': 'HBAR-USD', "Kraken": "HBARUSD"},
                     'DOT-USD': {'Binance': 'DOTUSDT', 'Coinbase': 'DOT-USD', 'Kraken': 'DOTUSD'},
                     'BCH-USD': {'Binance': 'BCHUSDT', 'Coinbase': 'BCH-USD', 'Kraken': 'BCHUSD'},
                     'UNI-USD': {'Binance': 'UNIUSDT', 'Coinbase': 'UNI-USD', 'Kraken': 'UNIUSD'},
                     'DAI-USD': {'Binance': 'DAIUSDT', 'Coinbase': 'DAI-USD', 'Kraken': 'DAIUSD'},
                     'APT-USD': {'Binance': 'APTUSDT', 'Coinbase': 'APT-USD', "Kraken": "APTUSD"},
                     'ONDO-USD': {'Binance': 'ONDOUSDT', 'Coinbase': 'ONDO-USD', "Kraken": "ONDOUSD"},
                     'AAVE-USD': {'Binance': 'AAVEUSDT', 'Coinbase': 'AAVE-USD', 'Kraken': 'AAVEUSD'},
                     'NEAR-USDT': {'Binance': 'NEARUSDTT', 'Coinbase': 'NEAR-USDT', 'Kraken': 'NEARUSD'},
                     'ICP-USD': {'Binance': 'ICPUSDT', 'Coinbase': 'ICP-USD', 'Kraken': 'ICPUSD'},
                     'ETC-USD': {'Binance': 'ETCUSDT', 'Coinbase': 'ETC-USD', 'Kraken': 'XETCZUSD'},
                     'VET-USD': {'Binance': 'VETUSDT', 'Coinbase': 'VET-USD', 'Kraken': 'VETUSD'},
                     'POL-USD': {'Binance': 'POLUSDT', 'Coinbase': 'POL-USD', "Kraken": "POLUSD"},
                     'CRO-USD': {'Binance': 'CROUSDT', 'Coinbase': 'CRO-USD', "Kraken": "CROUSD"},
                     'RENDER-USD': {'Binance': 'RENDERUSDT', 'Coinbase': 'RENDER-USD', "Kraken": "RENDERUSD"},
                     'ALGO-USD': {'Binance': 'ALGOUSDT', 'Coinbase': 'ALGO-USD', 'Kraken': 'ALGOUSD'},
                     'FIL-USD': {'Binance': 'FILUSDT', 'Coinbase': 'FIL-USD', 'Kraken': 'FILUSD'},
                     'ARB-USD': {'Binance': 'ARBUSDT', 'Coinbase': 'ARB-USD', "Kraken": "ARBUSD"},
                     'OP-USD': {'Binance': 'OPUSDT', 'Coinbase': 'OP-USD', "Kraken": "OPUSD"},
                     'ATOM-USD': {'Binance': 'ATOMUSDT', 'Coinbase': 'ATOM-USD', 'Kraken': 'ATOMUSD'},
                     'FET-USD': {'Binance': 'FETUSDT', 'Coinbase': 'FET-USD', "Kraken": "FETUSD"},
                     'TIA-USD': {'Binance': 'TIAUSDT', 'Coinbase': 'TIA-USD', "Kraken": "TIAUSD"},
                     'LDO-USD': {'Binance': 'LDOUSDT', 'Coinbase': 'LDO-USD', 'Kraken': 'LDOUSD'},
                     'STX-USD': {'Binance': 'STXUSDT', 'Coinbase': 'STX-USD', "Kraken": "STXUSD"},
                     'IMX-USD': {'Binance': 'IMXUSDT', 'Coinbase': 'IMX-USD', 'Kraken': 'IMXUSD'},
                     'GRT-USD': {'Binance': 'GRTUSDT', 'Coinbase': 'GRT-USD', 'Kraken': 'GRTUSD'},
                     'BONK-USD': {'Binance': 'BONKUSDT', 'Coinbase': 'BONK-USD', "Kraken": "BONKUSD"},
                     'FLR-USD': {'Binance': 'FLRUSDT', 'Coinbase': 'FLR-USD', "Kraken": "FLRUSD"},
                     'QNT-USD': {'Binance': 'QNTUSDT', 'Coinbase': 'QNT-USD', 'Kraken': 'QNTUSD'},
                     'SEI-USD': {'Binance': 'SEIUSDT', 'Coinbase': 'SEI-USD', "Kraken": "SEIUSD"},
                     'JASMY-USD': {'Binance': 'JASMYUSDT', 'Coinbase': 'JASMY-USD', "Kraken": "JASMYUSD"},
                     'FLOKI-USD': {'Binance': 'FLOKIUSDT', 'Coinbase': 'FLOKI-USD', "Kraken": "FLOKIUSD"},
                     'MKR-USD': {'Binance': 'MKRUSDT', 'Coinbase': 'MKR-USD', 'Kraken': 'MKRUSD'},
                     'EOS-USD': {'Binance': 'EOSUSDT', 'Coinbase': 'EOS-USD', 'Kraken': 'XEOSZUSD'},
                     'ENS-USD': {'Binance': 'ENSUSDT', 'Coinbase': 'ENS-USD', "Kraken": "ENSUSD"},
                     'XTZ-USD': {'Binance': 'XTZUSDT', 'Coinbase': 'XTZ-USD', 'Kraken': 'XTZUSD'},
                     'SAND-USD': {'Binance': 'SANDUSDT', 'Coinbase': 'SAND-USD', 'Kraken': 'SANDUSD'},
                     'FLOW-USD': {'Binance': 'FLOWUSDT', 'Coinbase': 'FLOW-USD', 'Kraken': 'FLOWUSD'},
                     'JTO-USD': {'Binance': 'JTOUSDT', 'Coinbase': 'JTO-USD', "Kraken": "JTOUSD"},
                     'XCN-USD': {'Binance': 'XCN-USDT', 'Coinbase': 'XCN-USD', "Kraken": "XCNUSD"},
                     'TRUMP-USD': {'Binance': 'TRUMP-USDT', 'Coinbase': 'TRUMP-USD', "Kraken": "TRUMPUSD"},
                     'PEPE-USD': {'Binance': 'PEPE-USDT', 'Coinbase': 'PEPE-USD', "Kraken": "PEPEUSD"},
                     'RONIN-USD': {'Binance': 'RONIN-USDT', 'Coinbase': 'RONIN-USD', "Kraken": "RONINUSD"}}

ENDPOINT_TO_LIMIT = {"Coinbase": 300,
                     "Kraken": 720,
                     "Binance": 1000}


def get_price_interval(exchange, url_str, symbol, start, end, interval):
    """
    Function to fetch prices of a given interval.
    :param exchange: The exchange to fetch the data from.
    :param symbol: The symbol to get the data for.
    :return: The fetched data.
    """
    try:
        if exchange == "Kraken":
            url = url_str.format(symbol, start, interval)
        elif exchange == "Binance":
            dt = datetime.strptime(start, "%Y-%m-%dT%H:%M:%S")
            start_timestamp_ms = int(dt.timestamp() * 1000)
            dt = datetime.strptime(end, "%Y-%m-%dT%H:%M:%S")
            end_timestamp_ms = int(dt.timestamp() * 1000)

            url = url_str.format(symbol, start_timestamp_ms, end_timestamp_ms, interval)
        else:
            url = url_str.format(symbol, start, end, interval)

        response = requests.get(url)
        data = response.json()
        return data

    except Exception as e:
        logging.error(f"Fehler beim Abrufen des Preises von {exchange}: {e}")
        print(e)
        return None


def batch_request_by_limits(start_datetime, end_datetime, max_items, api_endpoint, url_str, symbol, time_interval):
    start_dt: datetime = datetime.fromisoformat(start_datetime)
    end_dt = datetime.fromisoformat(end_datetime)

    interval_in_seconds = excahnge_to_interval_in_seconds[api_endpoint]
    total_seconds = float((end_dt - start_dt).total_seconds())
    total_items = total_seconds / interval_in_seconds
    request_count = math.ceil(total_items / max_items)

    if request_count == 0:
        request_count += 1

    interval_delta = timedelta(seconds=(total_seconds / request_count))
    if api_endpoint == "Kraken":
        if total_items > max_items:
            max_seconds = max_items * interval_in_seconds
            start_date_kraken = datetime.now() - timedelta(seconds=max_seconds)
            print("The requested data exceeds the time range limit for Kraken. \n"
                  "Will only return the 720 recent entries till " + str(start_date_kraken))
        return [get_price_interval(api_endpoint, url_str, symbol, start_datetime, "", time_interval)]

    data = []
    batch_start = start_dt
    for i in range(request_count):
        batch_end = batch_start + interval_delta
        batch_end_str = batch_end.strftime("%Y-%m-%dT%H:%M:%S")
        batch_start_str = batch_start.strftime("%Y-%m-%dT%H:%M:%S")
        data.append(get_price_interval(api_endpoint, url_str, symbol, batch_start_str, batch_end_str, time_interval))
        batch_start = batch_end

    return data


print("Trying to get price and volume for following coins: " + str(list(API_SYMBOL_MAPPIN.keys())))
print()


def handle_coinbase_data(interval):
    if not interval:
        print("Couldn't get data for " + coin + " from coinbase.")
        return

    coinbase_rows = []
    try:
        for batch in interval:
            for ts in reversed(batch):
                rw = [datetime.fromtimestamp(int(ts[0])), coin, ts[4], ts[5]]
                coinbase_rows.append(rw)
    except ValueError as e:
        print("Couldn't get data for " + coin + " from coinbase.")
        print(batch.get("message"))
        return

    return coinbase_rows


def handle_binance_data(interval):
    if not interval[0]:
        print("Couldn't get data for " + coin + " from binance.")
        return

    binance_rows = []

    try:
        for batch in interval:
            for ts in batch:
                rw = [datetime.fromtimestamp(int(ts[0]) / 1000), coin, ts[4], ts[5]]
                binance_rows.append(rw)
        return binance_rows
    except ValueError as e:
        print("Couldn't get data for " + coin + " from binance: " + batch.get("msg"))
        return


def handle_kraken_data(interval):
    if not interval:
        print("Couldn't get data for " + coin + " from kraken.")

    kraken_rows = []
    for batch in interval:
        if batch.get("error"):
            print("Couldn't get data for " + coin + " from kraken.")
            print(batch.get("error")[0])
            return None

        kraken_data = batch["result"][API_SYMBOL_MAPPIN[coin]["Kraken"]]

        for ts in kraken_data:
            rw = [datetime.fromtimestamp(int(ts[0])), coin, ts[4], ts[6]]
            kraken_rows.append(rw)

    return kraken_rows


def create_file(rows, api_endpoint):
    file_name = api_endpoint + "_" + excahnge_to_interval[api_endpoint] + "_" + start_date + "_" + end_date
    file_name = file_name.replace(":", "_")
    with open(file_name, mode="w", newline="") as file:
        writer = csv.writer(file)
        print("Created file " + file_name)
        writer.writerow(["Date", "Currency", "Price", "Volume"])
        for row in rows:
            writer.writerow(row)
    print("Successfully wrote data to file " + file_name)


for api_endpoint, url_str in api_endpoints.items():
    rows = []
    for coin in API_SYMBOL_MAPPIN.keys():
        try:
            symbol = API_SYMBOL_MAPPIN[coin][api_endpoint]
        except KeyError:
            print("Coin " + coin + " isn't defined for " + api_endpoint)
            continue

        data = batch_request_by_limits(start_date, end_date, ENDPOINT_TO_LIMIT[api_endpoint],
                                       api_endpoint, url_str, symbol, excahnge_to_interval[api_endpoint])

        if not data:
            print("Couldn't get data for " + coin)

        if api_endpoint == "Kraken":
            if data[0].get("error"):
                print("Error for coin " + symbol)
                print(data[0].get("error"))
                continue
            try:
                row = handle_kraken_data(data)
            except ValueError as e:
                print("The defined timerange for kraken is even with batching to big. "
                      "Define a new timerange.")
                continue
            if row is None:
                continue
            print("Fetched data for " + coin + " from " + api_endpoint)
            rows += row
        elif api_endpoint == "Coinbase":
            row = handle_coinbase_data(data)
            if row is None:
                continue
            print("Fetched data for " + coin + " from " + api_endpoint)
            rows += row
        else:
            row = handle_binance_data(data)
            if row is None:
                continue
            print("Fetched data for " + coin + " from " + api_endpoint)
            rows += row
    create_file(rows, api_endpoint)
