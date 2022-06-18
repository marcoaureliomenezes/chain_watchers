from datetime import datetime as dt
from brownie import network, interface
from sqlalchemy import create_engine
from scripts.utils import table_exists, insert_to_database
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from sqlalchemy.types import VARCHAR
import time

def compose_row(round_id, asset_attributes, pricefeed_response):
    return  {
            'network': asset_attributes['network'],
            'pair': asset_attributes['description'],
            'type': asset_attributes["tipo"],
            'address': asset_attributes["address"], 
            'price': pricefeed_response[1],
            'decimals': asset_attributes['decimals'],
            'round_id': str(round_id),
            'started_at': pricefeed_response[2],
            'updated_at': pricefeed_response[3],
            'answeredInRound': pricefeed_response[4]
    }

def find_holes(interval, rounds):
    df_all = pd.DataFrame([i for i in range(*interval)], columns=['whole'])
    df_real = pd.DataFrame(rounds, columns=['real'])
    result = pd.merge(df_all,df_real, left_on='whole', right_on='real' ,how='left')
    return result.loc[result['real'].isnull()].whole.values


def fulfill_assets_data(db_engine, aggregator_contract, interval, asset_attr):
    assets_list, counter, pair = ([], 0, asset_attr.get('pair'))
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    asset_attr['network'] = network.show_active()
    asset_attr['description'] = aggregator_contract.description()
    asset_attr['decimals'] = aggregator_contract.decimals()
    for round in interval:
        pricefeed_response = aggregator_contract.getRoundData(round)
        assets_list.append(compose_row(round, asset_attr, pricefeed_response))
        if counter == 100:
            date, asset_pair = (dt.fromtimestamp(pricefeed_response[2]), asset_attr['description'])
            print(f"Pair: {asset_pair}, Date: {date}")
            insert_to_database(db_engine, assets_list, table_name)
            assets_list, counter = ([], 0)
        counter += 1
    insert_to_database(db_engine, assets_list, table_name)


def watch_asset_price(db_engine, asset_attr):
    pair, address = (asset_attr.get('pair'), asset_attr.get('address'))
    pricefeed_contract = interface.AggregatorV3Interface(address)
    aggregator_contract = interface.AggregatorInterface(pricefeed_contract.aggregator())
    expected_interval = (0, aggregator_contract.latestRound())
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    if table_exists(db_engine, table_name):
        df_round_ids = pd.read_sql(f"SELECT round_id FROM {table_name}", con=db_engine)
        df_round_ids = df_round_ids.astype('int')
        gaps = find_holes(expected_interval, df_round_ids.round_id.values)
        if len(gaps == 0):
            return "Table is Up to Date"
        fulfill_assets_data(db_engine, aggregator_contract, gaps, asset_attr)
        return "Table Updated!"
    else:
        print(expected_interval)
        gaps = [round for round in range(*expected_interval)]
        fulfill_assets_data(db_engine, aggregator_contract, gaps, asset_attr)
        return "Table Created and Updated!"


def main(host, user, password, pair, name, tipo, address):
    engine_url = f'mysql+pymysql://{user}:{password}@{host}/oracles'
    asset_metadata = {'pair': pair, 'name': name, 'tipo': tipo, 'address': address}
    engine = create_engine(engine_url)
    if not database_exists(engine.url):
        create_database(engine.url)
    print("Starting...")
    res = watch_asset_price(engine, asset_metadata)
    print(res)

