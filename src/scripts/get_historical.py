from datetime import datetime
from brownie import network, interface
from sqlalchemy import create_engine
import csv, logging
import pandas as pd
from sqlalchemy.types import VARCHAR
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy_utils import database_exists, create_database


def get_addresses(assets_metadata): 
    with open(assets_metadata) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        res = [row for row in csv_reader]
    return res


def insert_to_database(db_engine, assets_list, table_name):
    dataframe = pd.DataFrame(assets_list)
    dataframe.to_sql(table_name, con=db_engine, if_exists='append', dtype=VARCHAR(256), index=False)

def analyze_table(db_engine, table_name):
    query_min = f'SELECT MIN(round_id) FROM {table_name}'
    query_max = f'SELECT MAX(round_id) FROM {table_name}'
    min, max = [int(pd.read_sql(query, con=db_engine).values[0][0]) for query in (query_min, query_max)]
    return min, max

def table_exists(db_engine, table_name):
    inspector = Inspector.from_engine(db_engine)
    return table_name in inspector.get_table_names()

def upstream_pricefeed(db_engine, table_name, actual_round, asset_attr):
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr["address"])
    max_round = pricefeed_contract.latestRoundData()[0]
    print(f"upstream acontecendo {table_name}")
    print(actual_round, max_round)
    assets, counter, price = ([], 0, 1)
    for round_id in range(actual_round, max_round):
        answer = pricefeed_contract.getRoundData(round_id)
        asset_data = {
            'network': network.show_active(),
            'pair': pricefeed_contract.description(), 
            'type': asset_attr["tipo"],
            'address': asset_attr["address"], 
            'price': answer[1],
            'decimals': pricefeed_contract.decimals(), 
            'round_id': str(round_id),
            'started_at': answer[2],
            'updated_at': answer[3],
            'answeredInRound': answer[4]
        }
        assets.append(asset_data)
        date, asset_pair = datetime.fromtimestamp(answer[2]), pricefeed_contract.description()
        print(f"Pair: {asset_pair}, Date: {date}")
    insert_to_database(db_engine, assets, table_name)
    print("Upstream Done!")
    return "Upstream Done!"

def downstream_pricefeed(db_engine, table_name, round_id, asset_attr):
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr["address"])
    assets, counter, price = ([], 0, 1)
    decimal = pricefeed_contract.decimals()
    description = pricefeed_contract.description()
    while price != 0:
        try:
            answer = pricefeed_contract.getRoundData(round_id)
        except:
            print("All historic price of asset {1} was obtained")
            break
        asset_data = {
            'network': network.show_active(),
            'pair': description, 
            'type': asset_attr["tipo"],
            'address': asset_attr["address"], 
            'price': answer[1],
            'decimals': decimal, 
            'round_id': str(round_id),
            'started_at': answer[2],
            'updated_at': answer[3],
            'answeredInRound': answer[4]
        }  
        assets.append(asset_data)
        date, asset_pair = datetime.fromtimestamp(answer[2]), pricefeed_contract.description()
        print(f"Pair: {asset_pair}, Date: {date}")
        round_id -= 1
        if counter == 100:
            insert_to_database(db_engine, assets, table_name)
            assets, counter = ([], 0)
        counter += 1
    insert_to_database(db_engine, assets, table_name)
    return "Downstream Done!"

def ingest_pricefeed_asset(db_engine, asset_attr):
    pair, address = (asset_attr.get('pair'), asset_attr.get('address'))
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    table_exists(db_engine, table_name)
    if table_exists(db_engine, table_name):
        print("table already exists. Starting from the minimum round!")
        min_round, max_round = analyze_table(db_engine, table_name)
        upstream_pricefeed(db_engine, table_name, max_round, asset_attr)
        downstream_pricefeed(db_engine, table_name, min_round, asset_attr)
    else:
        print("table doesn't exist. Starting from the maximum round!")
        pricefeed_contract = interface.AggregatorV3Interface(address)
        max_round = pricefeed_contract.latestRoundData()[0]
        downstream_pricefeed(db_engine, table_name, max_round, asset_attr)


def watch_asset_price(db_engine):
    assets_data = get_addresses(f'./scripts/data/{network.show_active()}.csv')
    for pair, name, tipo, address, in assets_data:
        asset_metadata = {'pair': pair, 'name': name, 'tipo': tipo, 'address': address}
        ingest_pricefeed_asset(db_engine, asset_metadata)
    print("Done!")


def main(host, user, password):
    engine_url = f'mysql+pymysql://{user}:{password}@{host}/oracles'
    engine = create_engine(engine_url)
    if not database_exists(engine.url):
        create_database(engine.url)
    watch_asset_price(engine)
