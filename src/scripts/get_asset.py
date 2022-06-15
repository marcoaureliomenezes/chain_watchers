from datetime import datetime as dt
from brownie import network, interface
from sqlalchemy import create_engine
from scripts.utils import insert_to_database,get_addresses, table_exists, analyse_rounds
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

def downstream_pricefeed(db_engine, table_name, round_id, asset_attr):
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr["address"])
    asset_attr['description'] = pricefeed_contract.description()
    asset_attr['decimals'] = pricefeed_contract.decimals()
    asset_attr['network'] = network.show_active()
    assets_list, counter, price = ([], 0, 1)
    while price != 0:
        try:
            pricefeed_response = pricefeed_contract.getRoundData(round_id - 1)
        except:
            print("All historic price of asset {1} was obtained")
            break
        assets_list.append(compose_row(round_id, asset_attr, pricefeed_response))
        round_id -= 1
        if counter == 100:
            date, asset_pair = (dt.fromtimestamp(pricefeed_response[2]), asset_attr['description'])
            print(f"Pair: {asset_pair}, Date: {date}")
            insert_to_database(db_engine, assets_list, table_name)
            assets_list, counter = ([], 0)
        counter += 1
    insert_to_database(db_engine, assets_list, table_name)
    return "Downstream Done!"


def upstream_pricefeed(db_engine, table_name, round_interval, asset_attr):
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr["address"])
    asset_attr['description'] = pricefeed_contract.description()
    asset_attr['decimals'] = pricefeed_contract.decimals()
    asset_attr['network'] = network.show_active()
    asset_list = []
    if round_interval[0] == round_interval[1]:
        print("It is already updated")
        return
    for round_id in range(round_interval[0], round_interval[1]+1):
        try:
            pricefeed_response = pricefeed_contract.getRoundData(round_id)
        except:
            print("ERRO AQUI")
        asset_list.append(compose_row(round_id, asset_attr, pricefeed_response))
        date, asset_pair = (dt.fromtimestamp(pricefeed_response[2]), asset_attr['description'])
        print(f"Pair: {asset_pair}, Date: {date} Updated")
    insert_to_database(db_engine, asset_list, table_name)
    return "Upstream Done!"


def fulfill_pricefeed(db_engine, table_name, round_interval, asset_attr):
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr["address"])
    asset_attr['description'] = pricefeed_contract.description()
    asset_attr['decimals'] = pricefeed_contract.decimals()
    asset_attr['network'] = network.show_active()
    asset_list = []
    if len(round_interval) == 0:
        return f"Table {table_name} has any hole!"
    for round_id in round_interval:
        print("FULLFILLNESS BEING DONE")
        pricefeed_response = pricefeed_contract.getRoundData(round_id)
        asset_list.append(compose_row(round_id, asset_attr, pricefeed_response))
        date, asset_pair = (dt.fromtimestamp(pricefeed_response[2]), asset_attr['description'])
        print(f"Pair: {asset_pair}, Date: {date} Updated")
        insert_to_database(db_engine, asset_list, table_name)
    return "Fulfill Done!"
    

def complete_pricefeed(db_engine, table_name, asset_attr):
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr["address"])
    asset_attr['description'] = pricefeed_contract.description()
    asset_attr['decimals'] = pricefeed_contract.decimals()
    print(f"Completing Stream happening on {table_name}")
    df_round_ids =  pd.read_sql(f"SELECT round_id FROM {table_name}", con=db_engine)
    upgrade, fulfillness, downgrade = analyse_rounds(df_round_ids, pricefeed_contract)
    print("INFO: UPGRADE.\tRounds to be upstream:")
    res_upgrade = upstream_pricefeed(db_engine, table_name, upgrade, asset_attr)
    print(res_upgrade)

    print("INFO: FULLFILL.\tRounds to be fullfil:")
    res_upgrade_fulfill = fulfill_pricefeed(db_engine, table_name, fulfillness, asset_attr)
    print(res_upgrade_fulfill)

    print("INFO: DOWNGRADING\tStarting from ")
    res_upgrade_downstream = downstream_pricefeed(db_engine, table_name, downgrade - 1, asset_attr)
    print(res_upgrade_downstream)


def remove_duplicated(db_engine, table_name):
    whole_table = pd.read_sql(f"SELECT * FROM {table_name}", con=db_engine)
    print("### REMOVENDO DUPLICADOS ###")
    print(whole_table.shape[0])
    cleaned_table = whole_table.drop_duplicates()
    print(cleaned_table.shape[0])
    cleaned_table.to_sql(table_name, con=db_engine, if_exists='replace', dtype=VARCHAR(256), index=False)
    time.sleep(2)


def ingest_pricefeed_asset(db_engine, asset_attr, network_active):
    pair, address = (asset_attr.get('pair'), asset_attr.get('address'))
    table_name = f"{pair}_{network_active.replace('-', '_')}"
    if table_exists(db_engine, table_name):
        print("table already exists. Starting from the minimum round!")
        remove_duplicated(db_engine, table_name)
        complete_pricefeed(db_engine, table_name, asset_attr)
    else:
        print("table doesn't exist. Starting from the maximum round!")
        pricefeed_contract = interface.AggregatorV3Interface(address)
        max_round = pricefeed_contract.latestRoundData()[0]
        downstream_pricefeed(db_engine, table_name, max_round, asset_attr)


def watch_asset_price(db_engine, asset_metadata):
    network_active = network.show_active()
    network_active = 'mainnet' if network_active == 'mainnet-fork' else network_active
    ingest_pricefeed_asset(db_engine, asset_metadata, network_active)


def main(host, user, password, pair, name, tipo, address):
    engine_url = f'mysql+pymysql://{user}:{password}@{host}/oracles'
    asset_metadata = {'pair': pair, 'name': name, 'tipo': tipo, 'address': address}
    engine = create_engine(engine_url)
    if not database_exists(engine.url):
        create_database(engine.url)
    print("Starting...")
    watch_asset_price(engine, asset_metadata)