from brownie import network, interface
from datetime import datetime as dt
from scripts.utils import insert_to_database
import csv, logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)

def compose_price_row(round_id, pricefeed_response):
    return  { 
            'round_id': round_id,
            'answeredInRound': pricefeed_response[4],
            'started_at': pricefeed_response[2],
            'updated_at': pricefeed_response[3],
            'price': pricefeed_response[1]
    }


def fulfill_assets_data(engine_url, interval,asset_attr):
    db_engine = create_engine(engine_url)
    assets_list, counter, pair = ([], 0, asset_attr.get('pair'))
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr['address'])
    asset_attr['description'] = pricefeed_contract.description()
    aggregator_contract = interface.AggregatorInterface(pricefeed_contract.aggregator())
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    for round in interval:
        try:
            pricefeed_response = aggregator_contract.getRoundData(round)
            assets_list.append(compose_price_row(round, pricefeed_response))
            date, asset_pair = (dt.fromtimestamp(pricefeed_response[2]), asset_attr['description'])
            logging.info(f"Pair: {asset_pair}, Date: {date}")
            if counter == 100:
                insert_to_database(db_engine, assets_list, table_name)
                assets_list, counter = ([], 0)
            counter += 1
        except:
            logging.info("Error hitting the API")
            continue
    insert_to_database(db_engine, assets_list, table_name)
    return "SIMPLE PRICEFEED INGESTION DONE!"


def read_round_ids(part_round_id, tmp_data_path):
    with open(tmp_data_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=';')
        result = [row for row in csv_reader]
    return [int(i) for i in result[int(part_round_id) - 1] ]

def main(db_string, pair, address, tmp_data_path, part_round_id):
    asset_attr = {'pair': pair, 'address': address}
    rounds_interval = read_round_ids(part_round_id, tmp_data_path)
    fulfill_assets_data(db_string, rounds_interval, asset_attr)
