from  many_users_work_copy import DB_URL, ScreenmarkFact
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
            db_url=DB_URL,
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
    for row in ScreenmarkFact.objects_in(connection).filter(ScreenmarkFact.department == '4271').only('marker'):
        print(row.marker)



def check():
    connection = process()
    reading(connection)



if __name__ == '__main__':
    check()