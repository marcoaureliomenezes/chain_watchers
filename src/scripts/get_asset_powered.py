from subprocess import Popen
from datetime import datetime as dt
from brownie import network, interface
from sqlalchemy import create_engine
from scripts.utils import table_exists, insert_to_database, divide_array
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import csv
from functools import reduce
import os

def compose_price_row(round_id, pricefeed_response):
    return  { 
            'round_id': round_id,
            'answeredInRound': pricefeed_response[4],
            'started_at': pricefeed_response[2],
            'updated_at': pricefeed_response[3],
            'price': pricefeed_response[1]
    }

def find_holes(interval, rounds):
    df_all = pd.DataFrame([i for i in range(*interval)], columns=['whole'])
    df_real = pd.DataFrame(rounds, columns=['real'])
    result = pd.merge(df_all,df_real, left_on='whole', right_on='real' ,how='left')
    return result.loc[result['real'].isnull()].whole.values

def run_concurrently(commands_list):
    procs = [ Popen(i) for i in commands_list ]
    for p in procs:
        p.wait()
    return "SUCCESS!!!"

def mother_function(engine_url, gaps, asset_attr, factor):
    base_command = f'brownie run scripts/ingestion.py main {engine_url}'
    assets_parm = reduce(lambda a, b: f'{a} {b}', asset_attr.values())
    gap_arrays = divide_array(gaps, factor)
    tmp_data_filename = f'{asset_attr["pair"]}_{network.show_active()}.csv'
    tmp_data_path = f'./scripts/tmp/{tmp_data_filename}'
    with open(tmp_data_path, 'w') as f:
        writer = csv.writer(f, delimiter=';')
        for row in gap_arrays:
            writer.writerow(row)
    commands_list = []
    for array_id in range(1, len(gap_arrays) + 1):
        part_id = f'{array_id}'
        command = f'{base_command} {assets_parm} {tmp_data_path} {part_id} --network {network.show_active()}'
        commands_list.append(command)
    supreme_list = [item.split(" ") for item in commands_list]
    run_concurrently(supreme_list)
    print("ACABOU")
    return "MOCK DONE"

def watch_asset_price(engine_url, asset_attr, factor):
    db_engine = create_engine(engine_url)
    pair, address = (asset_attr.get('pair'), asset_attr.get('address'))
    pricefeed_contract = interface.AggregatorV3Interface(address)
    aggregator_contract = interface.AggregatorInterface(pricefeed_contract.aggregator())
    expected_interval = (0, aggregator_contract.latestRound())
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    if table_exists(db_engine, table_name):
        df_round_ids = pd.read_sql(f"SELECT round_id FROM {table_name}", con=db_engine)
        df_round_ids = df_round_ids.astype('int')
        gaps = find_holes(expected_interval, df_round_ids.round_id.values)
        if len(gaps) == 0:
            return "Table is Up to Date"
        mother_function(engine_url, gaps, asset_attr, factor)
        return "Table Updated!"
    else:
        print(expected_interval)
        gaps = [round for round in range(*expected_interval)]
        mother_function(engine_url, gaps, asset_attr, factor)
        return "Table Created and Updated!"


def main(host, user, password, pair, name, tipo, address, factor):
    engine_url = f'mysql+pymysql://{user}:{password}@{host}/oracles'
    asset_metadata = {'pair': pair, 'name': name, 'tipo': tipo, 'address': address}
    engine = create_engine(engine_url)
    if not database_exists(engine.url):
        create_database(engine.url)
    print("Starting...")
    res = watch_asset_price(engine_url, asset_metadata, int(factor))
    print(res)

