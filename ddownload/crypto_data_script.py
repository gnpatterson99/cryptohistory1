import csv
import logging
from datetime import datetime

import requests

# Logging-Konfiguration
logging.basicConfig(filename='crypto_data.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

logging.info("Programm started")

#start_date = "2025-02-01T00:00:00"
#end_date = "2025-02-28T00:00:00"
start_date = "2025-01-01"
end_date = "2025-01-10"

file_name = "output.csv"
api_endpoints = {
    'Coinbase': "https://api.exchange.coinbase.com/products/{}/candles?start={}&end={}&granularity=3600"

}
# ink, trump, pepe, ronin
assets = [
    "BTC_USD", "ETH_USD", "XRP_USD", "SOL_USD", "DOGE_USD", "ADA_USD", "LINK_USD", "XLM_USD",
    "LTC_USD", "SUI_USD", "AVAX_USD", "SHIB_USD", "HBAR_USD", "DOT_USD", "BCH_USD", "UNI_USD",
    "DAI_USD", "APT_USD", "ONDO_USD", "AAVE_USD", "NEAR_USDT", "ICP_USD",
    "ETC_USD", "VET_USD", "POL_USD", "CRO_USD", "RENDER_USD", "ALGO_USD", "FIL_USD", "ARB_USD",
    "OP_USD", "ATOM_USD", "FET_USD", "TIA_USD", "LDO_USD", "STX_USD", "IMX_USD",
    "GRT_USD", "BONK_USD", "FLR_USD", "QNT_USD", "SEI_USD", "JASMY_USD", "FLOKI_USD", "MKR_USD",
    "EOS_USD", "ENS_USD", "XTZ_USD", "SAND_USD", "FLOW_USD", "JTO_USD", "RONIN_USD", "XCN-USD", "TRUMP-USD",
    "PEPE-USD", "RONIN-USD"
]


def get_price_interval(exchange, symbol, start_date_str, end_date_str):
    """
    Function to fetch prices of a given interval.
    :param exchange: The exchange to fetch the data from.
    :param symbol: The symbol to get the data for.
    :return: The fetched data.
    """
    try:
        url = api_endpoints[exchange].format(symbol, start_date_str, end_date_str)
        response = requests.get(url)
        data = response.json()
        return data

    except Exception as e:
        logging.error(f"Error accessing exchange {exchange} for symbol {symbol} Exception: {e}")
        return None


def loop_dates_1(start_date, end_date, file_name, assets):

#    for date in start_date,

    local_assets = assets.copy()

    print("Trying to get price and volume for following coins: " + str(assets))

    with open(file_name, mode="w", newline="") as file:
        writer = csv.writer(file)
        print("Created file " + file_name)
        writer.writerow(["Date", "Currency", "Low", "High", "Open", "Close", "Volume"])
        for coin in local_assets:
            coin = coin.replace("_", "-")
            for exchange in api_endpoints.keys():
                interval = get_price_interval(exchange, coin)
                if not interval:
                    print("Couldn't get data for " + coin + "Removing it from the queue")
                    local_assets.remove(coin)
                    continue

                print("Fetched data for " + coin)
                try:
                    for timestamp in interval:
                        row = [datetime.fromtimestamp(int(timestamp[0])), coin, timestamp[1], timestamp[2], timestamp[3], timestamp[4], timestamp[5]]
                        writer.writerow(row)
                except Exception as e:
                    print("There was an error getting data for " + coin)
                    continue

    print("Successfully wrote data to file " + file_name)


from datetime import datetime, timedelta


def loop_dates(start_date_str, end_date_str, file_name, assets):
    """
    This function loops through a range of dates (one day frequency) and fetches cryptocurrency data.
    """
    # Parse the start and end dates
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    local_assets = assets.copy()

    print("Trying to get price and volume for the following coins: " + str(assets))

    with open(file_name, mode="w", newline="") as file:
        writer = csv.writer(file)
        print("Created file " + file_name)
        writer.writerow(["Date", "Currency", "Low", "High", "Open", "Close", "Volume"])

        # Loop through the dates with a one-day interval
        current_date = end_date
        while current_date >= start_date:
            prior_date = current_date - timedelta(days=12)

            # Loop through all assets to fetch data for the current date range
            for coin in local_assets:
                coin_symbol = coin.replace("_", "-")
                for exchange in api_endpoints.keys():
                    interval_data = get_price_interval(
                        exchange,
                        coin_symbol,
                        prior_date.strftime("%Y-%m-%dT00:00:00"),
                        current_date.strftime("%Y-%m-%dT00:00:00"),
                    )
                    if not interval_data:
                        print(f"Couldn't get data for {coin_symbol} on {prior_date.strftime('%Yh-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")
                        try:
                            # TODO: use the original symbol, not the one after cleanup

                            local_assets.remove(coin_symbol)
                        except Exception as e:
                            print(f"Error removing {coin_symbol}")
                            logging.error(f"Error removing {coin_symbol}: {e}")
                        continue

                    print(f"Fetched data for {coin_symbol} for range {prior_date.strftime('%Yh-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")
                    try:
                        for timestamp in interval_data:
                            row = [
                                datetime.fromtimestamp(int(timestamp[0])),
                                coin_symbol,
                                timestamp[1],  # Low
                                timestamp[2],  # High
                                timestamp[3],  # Open
                                timestamp[4],  # Close
                                timestamp[5],  # Volume
                            ]
                            writer.writerow(row)
                    except Exception as e:
                        print(
                            f"There was an error processing data for {coin_symbol} on {current_date.strftime('%Y-%m-%d')}")
                        logging.error(f"Error processing data for {coin_symbol}: {e}")
                        continue

            # Move to the next date
            current_date = prior_date

    print(f"Successfully wrote data to file {file_name}")


if __name__ == "__main__":
    loop_dates("2024-01-01", "2024-12-31", "2024.t2.year.csv", assets)


