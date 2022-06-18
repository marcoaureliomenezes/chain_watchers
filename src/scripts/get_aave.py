from datetime import datetime as dt
from brownie import network, interface
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
from sqlalchemy.types import VARCHAR
import time


# 0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5
aave_v2 = {
    'address_provider': interface.ILendingPoolAddressesProvider,
    'pool_contract': interface.ILendingPool
}

aave_v3 = {
    'address_provider': interface.IPoolAddressesProvider,
    'pool_contract': interface.IPool
}

def get_aave(engine, entrypoint_address, version='v3'):
    interfaces = aave_v3 if version == 'v3' else aave_v2
    lending_pool_provider = interfaces['address_provider'](entrypoint_address)
    print(lending_pool_provider)
    lending_pool_address = lending_pool_provider.getLendingPool()
    print(lending_pool_address)
    lending_pool = interfaces['pool_contract'](lending_pool_address)
    reserves_list = lending_pool.getReservesList()
    print(reserves_list)
    for asset in reserves_list:
        erc_20_token = interface.IERC20(asset)
        try:
            asset_data = {
                'name': erc_20_token.name(),
                'symbol': erc_20_token.symbol(),
                'address': asset,
                'total_supply': erc_20_token.totalSupply()
            }
        except OverflowError as of:
            asset_data = {
                'name': 'ERROR',
                'symbol': 'ERROR',
                'address': asset,
                'total_supply': erc_20_token.totalSupply()
            }

        print(asset_data)
        

def main(host, user, password, provider_address):
    engine_url = f'mysql+pymysql://{user}:{password}@{host}/defi'
    engine = create_engine(engine_url)
    if not database_exists(engine.url):
        create_database(engine.url)
    print("Starting...")
    print(provider_address)
    get_aave(engine, provider_address, version = 'v2')

