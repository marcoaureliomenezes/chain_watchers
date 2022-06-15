import pandas as pd
import csv
from sqlalchemy.types import VARCHAR
from sqlalchemy.engine.reflection import Inspector


def table_exists(db_engine, table_name):
    inspector = Inspector.from_engine(db_engine)
    return table_name in inspector.get_table_names()


def get_addresses(assets_metadata): 
    with open(assets_metadata) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        res = [row for row in csv_reader]
    return res


def insert_to_database(db_engine, assets_list, table_name):
    dataframe = pd.DataFrame(assets_list)
    dataframe.to_sql(table_name, con=db_engine, if_exists='append', dtype=VARCHAR(256), index=False)

def find_missing_rounds(df_round_ids_list):
    minimo, maximo = (min(df_round_ids_list), max(df_round_ids_list))
    cond_complete = maximo - minimo + 1 == len(df_round_ids_list)
    if cond_complete:
        print("Any missing round")
        return []
    else:
        whole_data = [i for i in range(minimo, maximo + 1)]
        return [i for i in whole_data if i not in df_round_ids_list]


def analyse_rounds(df_round_ids, pricefeed_contract):
    df_round_ids_list = [int(i) for i in df_round_ids['round_id'].values]
    min_upgrade = max(df_round_ids_list) + 1
    max_upgrade = int(pricefeed_contract.latestRoundData()[0])
    upgrade = min_upgrade, max_upgrade
    fulfillness = find_missing_rounds(df_round_ids_list)
    max_downgrade = min(df_round_ids_list) - 1
    return upgrade, fulfillness, max_downgrade
 

