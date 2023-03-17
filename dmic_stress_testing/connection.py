from dmic_stress_testing.script_parser import read_config
import infi.clickhouse_orm as ico
import logging
import time
from dmic_stress_testing.p_database import p_db


def connect(configuration):
    try:
        uname_ = 'admin'
        pass_ = f'yuramarkin'
        connection = p_db(
            'dmic',
            db_url=configuration['DB_URL'],
            username=uname_,
            password=pass_
            )
        return True, connection
    except Exception as ex_:
        print(ex_)
    return False, False


def process(configuration):
    connected = False
    c = 0
    while not connected and c < 3:
        c += 1
        connected, connection = connect(configuration)
        if not connected:
            time.sleep(1)
    return connection

