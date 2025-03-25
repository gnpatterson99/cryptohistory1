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
start_date = "2010-01-01"
end_date = "2025-03-19"

file_name = "missing_coinbase_1.csv"
api_endpoints = {
    'Coinbase': "https://api.exchange.coinbase.com/products/{}/candles?start={}&end={}&granularity=86400"

}
# ink, trump, pepe, ronin
# assets = [
#     "BTC_USD", "ETH_USD", "XRP_USD", "SOL_USD", "DOGE_USD", "ADA_USD", "LINK_USD", "XLM_USD",
#     "LTC_USD", "SUI_USD", "AVAX_USD", "SHIB_USD", "HBAR_USD", "DOT_USD", "BCH_USD", "UNI_USD",
#     "DAI_USD", "APT_USD", "ONDO_USD", "AAVE_USD", "NEAR_USDT", "ICP_USD",
#     "ETC_USD", "VET_USD", "POL_USD", "CRO_USD", "RENDER_USD", "ALGO_USD", "FIL_USD", "ARB_USD",
#     "OP_USD", "ATOM_USD", "FET_USD", "TIA_USD", "LDO_USD", "STX_USD", "IMX_USD",
#     "GRT_USD", "BONK_USD", "FLR_USD", "QNT_USD", "SEI_USD", "JASMY_USD", "FLOKI_USD", "MKR_USD",
#     "EOS_USD", "ENS_USD", "XTZ_USD", "SAND_USD", "FLOW_USD", "JTO_USD", "RONIN_USD", "XCN-USD", "TRUMP-USD",
#     "PEPE-USD", "RONIN-USD"
# ]

assets=['CTX-USD', 'ATA-USD', 'GFI-USD', 'INJ-USD', 'STORJ-USD', 'MIR-USD', 'TONE-USD', 'OMNI-USD', 'YFII-USD', 'KEEP-USD',
        'REZ-USD', 'OXT-USD', 'SPELL-USD', 'ENJ-USD', 'ORCA-USD', 'SYN-USD', 'KARRAT-USD', 'DEXT-USD', 'ZEC-USD', 'CGLD-USD', 'MATH-USD',
        'OGN-USD', 'ANT-USD', 'KRL-USD', 'CVC-USD', 'GHST-USD', 'NEAR-USD', 'QSP-USD', 'COOKIE-USD', 'DYP-USD', 'CRV-USD', 'SUKU-USD',
        'T-USD', 'BLAST-USD', 'TVK-USD', 'WELL-USD', 'SHDW-USD', 'CBETH-USD', 'POPCAT-USD', 'DASH-USD', 'BADGER-USD',
        'RNDR-USD', 'ACH-USD', 'NEON-USD', 'LCX-USD', 'UPI-USD', 'RPL-USD', 'FIDA-USD', 'TRAC-USD', 'MOVE-USD', 'MAGIC-USD', 'BLZ-USD',
        'ALICE-USD', 'LRC-USD', 'AGLD-USD', 'LOOM-USD', 'MINA-USD', 'XYO-USD', 'SAFE-USD', 'SUSHI-USD', 'LQTY-USD', 'WBTC-USD', 'TRU-USD',
        'CVX-USD', 'CORECHAIN-USD', 'STRK-USD', 'PNUT-USD', 'BAT-USD', 'EIGEN-USD', 'MLN-USD', 'GMT-USD', 'PUNDIX-USD', 'SUPER-USD', 'MPL-USD',
        'POLS-USD', 'RLC-USD', 'FIS-USD', 'ZRO-USD', 'ACS-USD', 'BERA-USD', 'HONEY-USD', 'BICO-USD', 'RGT-USD', 'BAL-USD', 'DAR-USD', 'LPT-USD',
        'TIME-USD', 'API3-USD', 'GST-USD', 'LRDS-USD', 'IDEX-USD', '1INCH-USD', 'SKL-USD', 'SYRUP-USD', 'HOPR-USD', 'FORTH-USD', 'AXL-USD',
        'ROSE-USD', 'VOXEL-USD', 'TRB-USD', 'AUCTION-USD', 'ATH-USD', 'TNSR-USD',  'WCFG-USD', 'VTHO-USD', 'HNT-USD', 'NU-USD',
        'AMP-USD', 'PNG-USD', 'ACX-USD', 'COTI-USD', 'MONA-USD', 'B3-USD', 'MXC-USD', 'AVT-USD', 'BAND-USD', '00-USD', 'STG-USD', 'POWR-USD',
        'LOKA-USD', 'DIMO-USD', 'G-USD', 'UST-USD', 'BOBA-USD', 'GNO-USD', 'SHPING-USD', 'MDT-USD', 'MEDIA-USD', 'IOTX-USD',
        'DESO-USD', 'AIOZ-USD', 'PENGU-USD', 'MTL-USD', 'GALA-USD', 'WLUNA-USD', 'FOX-USD', 'MORPHO-USD', 'UNFI-USD', 'NCT-USD', 'AXS-USD',
        'HIGH-USD', 'ZETACHAIN-USD', 'PYTH-USD', 'ARPA-USD', 'DNT-USD', 'MSOL-USD', 'CTSI-USD', 'SWELL-USD', 'QUICK-USD', 'DIA-USD', 'AUDIO-USD',
        'BIT-USD', 'ZK-USD', 'MCO2-USD', 'ALEO-USD', 'BNT-USD', 'FORT-USD', 'ALCX-USD', 'MUSE-USD', 'POND-USD', 'DEGEN-USD', 'COVAL-USD', 'YFI-USD',
        'PLU-USD', 'HFT-USD', 'ASM-USD', 'KSM-USD', 'BTRST-USD', 'GLM-USD', 'PRQ-USD', 'VARA-USD', 'SWFTC-USD', 'LIT-USD', 'PYR-USD',
        'C98-USD', 'GODS-USD', 'KNC-USD', 'PLA-USD', 'SEAM-USD', 'ZRX-USD', 'RARI-USD', 'POLY-USD', 'MOODENG-USD', 'OMG-USD', 'APE-USD', 'A8-USD',
        'NKN-USD', 'ZETA-USD',  'ZEN-USD', 'REN-USD', 'PRO-USD', 'VGX-USD', 'AERGO-USD', 'RBN-USD', 'SNX-USD', 'REP-USD', 'IO-USD',
        'CRPT-USD', 'RARE-USD',  'ARKM-USD', 'COW-USD', 'RLY-USD', 'ME-USD', 'DDX-USD', 'LSETH-USD', 'CELR-USD', 'WIF-USD', 'MNDE-USD',
        'OOKI-USD', 'WAMPL-USD', 'PERP-USD', 'PIRATE-USD', 'SYLO-USD', 'CHZ-USD', 'AKT-USD', 'BOND-USD', 'QI-USD', 'METIS-USD',
        'OSMO-USD', 'ELA-USD', 'FARM-USD', 'JUP-USD', 'TRIBE-USD', 'INDEX-USD', 'OCEAN-USD', 'AERO-USD', 'PRCL-USD', 'ANKR-USD', 'KAVA-USD',
        'ETHFI-USD', 'PRIME-USD', 'MANA-USD', 'EGLD-USD', 'BIGTIME-USD', 'RAD-USD', 'KAITO-USD', 'CLV-USD', 'BLUR-USD', 'GAL-USD',
        'TAO-USD', 'REQ-USD', 'AST-USD', 'ILV-USD', 'TURBO-USD', 'ORN-USD', 'FX-USD', 'AURORA-USD', 'DRIFT-USD', 'VELO-USD', 'NMR-USD', 'MATIC-USD',
        'MASK-USD', 'GTC-USD', 'VVV-USD', 'INV-USD', 'SPA-USD', 'RED-USD', 'WAXL-USD', 'ERN-USD', 'SD-USD', 'GIGA-USD', 'SNT-USD', 'MULTI-USD',
        'NEST-USD', 'COMP-USD', 'ABT-USD', 'MOBILE-USD', 'UMA-USD', 'RAI-USD', 'MOG-USD', 'ALEPH-USD', 'IP-USD', 'DREP-USD', 'TOSHI-USD']



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

    local_assets = []
    for coin in assets:
        coin_symbol = coin.replace("_", "-")
        local_assets.append(coin_symbol)

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
#                coin_symbol = coin.replace("_", "-")
                coin_symbol = coin
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

                    print(f"Fetched data for {coin_symbol} for range {prior_date.strftime('%Y-%m-%d')} to {current_date.strftime('%Y-%m-%d')}")
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
    # loop_dates("2024-01-01", "2024-12-31", "2024.t2.year.csv", assets)
    # loop_dates("2023-09-01", "2023-12-31", "2023.q4.csv", assets)
    # loop_dates("2023-06-01", "2023-08-31", "2023.q3.csv", assets)
    # loop_dates("2023-02-27", "2023-05-31", "2023.q2.csv", assets)
    # loop_dates("2022-12-31", "2023-02-28", "2023.q1.csv", assets)
    # loop_dates("2021-12-31", "2022-12-31", "2022.year.csv", assets)
    # loop_dates("2021-12-31", "2022-11-01", "2022b.year.csv", assets)
    # loop_dates("2023-02-21", "2023-03-06", "data/backfill.1.csv", assets)
    # loop_dates("2024-03-09", "2024-03-11", "data/backfill.2.csv", assets)

    loop_dates("2010-01-01", "2025-03-19", "data/missing_coinbase_1.csv", assets)



