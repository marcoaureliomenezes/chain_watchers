from brownie import network, interface
from datetime import datetime as dt
from scripts.utils import insert_to_database
import argparse
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

def fulfill_assets_data(engine_url,asset_attr, interval):
    db_engine = create_engine(engine_url)
    assets_list, counter, pair = ([], 0, asset_attr.get('pair'))
    pricefeed_contract = interface.AggregatorV3Interface(asset_attr['address'])
    aggregator_contract = interface.AggregatorInterface(pricefeed_contract.aggregator())
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


def read_round_ids(part_round_id):
    with open('./scripts/tmp/{}')

def main(db_string, pair, name, tipo, address, part_round_id):
    asset_attr = {
        'pair': pair, 
        'name': name, 
        'tipo': tipo, 
        'address': address
    }
    rounds = read_round_ids(part_round_id, pair_network)

    
    #fulfill_assets_data(parm_db_string, interval, asset_attr)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='python arguments')
    parser.add_argument('--db_string', help= 'database connection string', required=True)
    parser.add_argument('--pair', help= 'asset pair', required=True)
    parser.add_argument('--name', help= 'asset name', required=True)
    parser.add_argument('--tipo', help= 'asset type', required=True)
    parser.add_argument('--address', help= 'pricefeed contract address', required=True)
    parser.add_argument('--round_list', help= 'List of round Ids to be got', nargs="*", required=True)

    args = parser.parse_args()

    parm_db_string = args.db_string
    parm_pair = args.pair
    parm_name = args.name
    parm_tipo = args.tipo
    parm_address = args.address
    parm_round_list = args.round_list
    asset_attr = {
        'pair': parm_pair, 
        'name': parm_name, 
        'tipo': parm_tipo, 
        'address': parm_address
    }
    print(parm_round_list)
    fulfill_assets_data(parm_db_string, interval, asset_attr)