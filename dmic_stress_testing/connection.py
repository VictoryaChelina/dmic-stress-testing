from dmic_stress_testing.common import read_config
import infi.clickhouse_orm as ico
from requests_toolbelt.adapters.source import SourceAddressAdapter
import requests
import logging
import time


def connect(name):
    configuration = read_config()
    source_adapter = SourceAddressAdapter('127.0.0.1')
    try:
        uname_ = name
        if name == 'admin':
            pass_ = f'yuramarkin'
            session = requests.Session()
            session.mount('http://', source_adapter)
            connection = ico.Database(
                'dmic',
                db_url=configuration['DB_URL'],
                username=uname_,
                password=pass_,
                request_session=session)
        else: 
            connection = ico.Database(
                'dmic',
                db_url=configuration['DB_URL'],
                username=uname_)
        logging.info(f'{uname_}: Подключился базе')
        return True, connection
    except Exception as ex_:
        logging.info(f'{uname_} {pass_}: Подключение...')
    return False, False


def process(name='admin'):
    connected = False
    c = 0
    while not connected and c < 3:
        c += 1
        connected, connection = connect(name)
        if not connected:
            time.sleep(1)
    return connection