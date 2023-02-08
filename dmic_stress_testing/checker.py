from dmic_stress_testing.many_users_work import ScreenmarkFact
from dmic_stress_testing.common import read_config
import logging
import infi.clickhouse_orm as ico
import time
import datetime
import traceback


logging.basicConfig(level=logging.DEBUG)


def connect(name):
    configuration = read_config()
    try:
        uname_ = name
        if name == 'admin':
            pass_ = f'yuramarkin'
            connection = ico.Database(
                'dmic',
                db_url=configuration['DB_URL'],
                username=uname_,
                password=pass_)
        else: 
            connection = ico.Database(
                'dmic',
                db_url=configuration['DB_URL'],
                username=uname_)
        logging.info(f'{uname_}: Подключился базе')
        return True, connection
    except Exception as ex_:
        logging.info(f'{uname_} {pass_}: Подключение...')
        traceback.print_exc()
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


def reading(connection):
    for row in ScreenmarkFact.objects_in(connection).filter(
            ScreenmarkFact.dt == datetime.date.today()):
        print(row.report_time)


def counting(connection, model=ScreenmarkFact):
    rows = connection.count(model)
    return rows


def realtime_counting(connection):
    while True:
        print(f'rows in base: {connection.count(ScreenmarkFact)}', end='\r')


def drop(connection):
    from infi.clickhouse_orm import migrations
    migrations.DropTable(ScreenmarkFact)


def check():
    connection = process()
    # realtime_counting(connection)
    # reading(connection)
    rows = counting(connection)
    print(rows)


if __name__ == '__main__':
    check()
