from  dmic_stress_testing.many_users_work import ScreenmarkFact
import logging
import infi.clickhouse_orm as ico
import time
import datetime
import traceback

logging.basicConfig(level=logging.DEBUG)

def connect():
    try:
        uname_ = f'admin'
        pass_ = f'yuramarkin'
        connection = ico.Database(
            'dmic',
            db_url="http://10.11.20.98:8123",
            username=uname_,
            password=pass_)
        logging.info(f'{uname_} {pass_}: Подключился базе')
        return True, connection
    except Exception as ex_:
        logging.info(f'{uname_} {pass_}: Подключение...')
        traceback.print_exc()
    return False, False


def process():
    connected = False
    while not connected:
        connected, connection = connect()
        if not connected:
            time.sleep(1)
    return connection
    

def reading(connection):
    for row in ScreenmarkFact.objects_in(connection).filter(ScreenmarkFact.dt == datetime.date.today()):
        print(row.report_time)


def counting(connection):
    rows = connection.count(ScreenmarkFact)
    return rows

def realtime_counting(connection):
    while True:
        print(f'rows in base: {connection.count(ScreenmarkFact)}', end='\r')

def check():
    connection = process()
    #realtime_counting(connection)
    #reading(connection)
    rows = counting(connection)
    print(rows)


if __name__ == '__main__':
    check()