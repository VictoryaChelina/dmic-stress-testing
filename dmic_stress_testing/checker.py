from  dmic_stress_testing.many_users_work import ScreenmarkFact
import logging
import infi.clickhouse_orm as ico
import time

logging.basicConfig(level=logging.INFO)

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
        #traceback.print_exc()
    return False


def process():
    connected = False
    while not connected:
        connected, connection = connect()
        if not connected:
            time.sleep(1)
    return connection
    

def reading(connection):
    for row in ScreenmarkFact.objects_in(connection).filter(ScreenmarkFact.user_name == '_').only('user_name'):
        print(row.user_name)


def counting(connection):
    rows = connection.count(ScreenmarkFact)
    return rows


def check():
    connection = process()
    reading(connection)
    rows = counting(connection)
    print(rows)


if __name__ == '__main__':
    check()