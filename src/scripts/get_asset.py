from brownie import network, interface
from sqlalchemy import create_engine
from scripts.utils import table_exists,add_metadata, find_holes, divide_array, run_concurrently
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import os, csv, logging
from scripts.ingestion import fulfill_assets_data

logging.basicConfig(level=logging.INFO)

def powered_fulfill_assets_data(engine_url, gaps, asset_attr, factor):
    base_command = f'brownie run scripts/ingestion.py main {engine_url}'
    asset_parms = f'{asset_attr["pair"]} {asset_attr["address"]}'
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
        command = f'{base_command} {asset_parms} {tmp_data_path} {part_id} --network {network.show_active()}'
        commands_list.append(command)
    supreme_list = [item.split(" ") for item in commands_list]
    run_concurrently(supreme_list)
    os.remove(tmp_data_path)
    return "POWERED PRICEFEED INGESTION DONE!"


def watch_asset_price(engine_url, pair, address, factor):
    db_engine = db_engine = create_engine(engine_url)
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    pricefeed_contract = interface.AggregatorV3Interface(address)
    asset_attr = add_metadata(db_engine, table_name, pair, pricefeed_contract)
    aggregator_contract = interface.AggregatorInterface(pricefeed_contract.aggregator())
    expected_interval = (0, aggregator_contract.latestRound())
    logging.info(f"COMPLETING TABLE {table_name}")
    if table_exists(db_engine, table_name):
        df_round_ids = pd.read_sql(f"SELECT round_id FROM {table_name}", con=db_engine)
        df_round_ids = df_round_ids.astype('int')
        interval = find_holes(expected_interval, df_round_ids.round_id.values)
    else:
        interval = [round for round in range(*expected_interval)]
    if len(interval) == 0:
        return "Table is Up to Date"
    logging.info(f'Updating Historical Price of {table_name}!')
    return fulfill_assets_data(engine_url, interval, asset_attr) if factor == 1 else \
                            powered_fulfill_assets_data(engine_url, interval, asset_attr, factor)


def main(pair, address, factor="1"):
    service, user, pwd = [os.getenv(i) for i in ('MYSQL_SERVICE', 'MYSQL_USER', 'MYSQL_PASS')]
    engine_url = f'mysql+pymysql://{user}:{pwd}@{service}/oracles'
    db_engine = create_engine(engine_url)
    if not database_exists(db_engine.url):
        create_database(db_engine.url)
    logging.info("STARTING...")
    res = watch_asset_price(engine_url, pair, address, int(factor))
    logging.info(f"LOGGING {res}")

