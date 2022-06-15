from datetime import datetime as dt
from brownie import network, interface
from sqlalchemy import create_engine
from scripts.utils import insert_to_database,get_addresses, table_exists, analyse_rounds
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from sqlalchemy.types import VARCHAR
import time


def watch_asset_price(db_engine, asset_metadata):
    network_active = network.show_active()
    network_active = 'mainnet' if network_active == 'mainnet-fork' else network_active
    print("table doesn't exist. Starting from the maximum round!")
    pricefeed_contract = interface.AggregatorV3Interface(asset_metadata["address"])
    max_round, max_round_price, _, _, _ = pricefeed_contract.latestRoundData()
    half_round = int(max_round/2)
    print(max_round, max_round_price)
    FLAG1, FLAG2 = (False, False)
    try: 
        half_round_price = pricefeed_contract.getRoundData(half_round)
        print(half_round, half_round_price)
    except:
        print("DEU ERRO")

def main(host, user, password, pair, name, tipo, address):
    engine_url = f'mysql+pymysql://{user}:{password}@{host}/oracles'
    asset_metadata = {'pair': pair, 'name': name, 'tipo': tipo, 'address': address}
    engine = create_engine(engine_url)
    if not database_exists(engine.url):
        create_database(engine.url)
    print("Starting...")
    watch_asset_price(engine, asset_metadata)

