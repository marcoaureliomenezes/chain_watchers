from brownie import network, interface
from datetime import datetime as dt
from scripts.utils import insert_to_database
import csv
from sqlalchemy import create_engine


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

def fulfill_assets_data(engine_url, interval,asset_attr):
    db_engine = create_engine(engine_url)
    assets_list, counter, pair = ([], 0, asset_attr.get('pair'))
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr['address'])
    aggregator_contract = interface.AggregatorInterface(pricefeed_contract.aggregator())
    table_name = f"{pair}_{network.show_active().replace('-', '_')}"
    asset_attr['network'] = network.show_active()
    asset_attr['description'] = aggregator_contract.description()
    asset_attr['decimals'] = aggregator_contract.decimals()
    for round in interval:
        try:
            pricefeed_response = aggregator_contract.getRoundData(round)
            assets_list.append(compose_row(round, asset_attr, pricefeed_response))
            if counter == 100:
                date, asset_pair = (dt.fromtimestamp(pricefeed_response[2]), asset_attr['description'])
                print(f"Pair: {asset_pair}, Date: {date}")
                insert_to_database(db_engine, assets_list, table_name)
                assets_list, counter = ([], 0)
            counter += 1
        except:
            print("Error hitting the API")
            continue
    insert_to_database(db_engine, assets_list, table_name)


def read_round_ids(part_round_id, tmp_data_path):
    with open(tmp_data_path, 'r') as f:
        csv_reader = csv.reader(f, delimiter=';')
        result = [row for row in csv_reader]
    return [int(i) for i in result[int(part_round_id) - 1] ]

def main(db_string, pair, name, tipo, address, tmp_data_path, part_round_id):
    asset_attr = {
        'pair': pair, 
        'name': name, 
        'tipo': tipo, 
        'address': address
    }
    rounds_interval = read_round_ids(part_round_id, tmp_data_path)
 
    fulfill_assets_data(db_string, rounds_interval, asset_attr)
