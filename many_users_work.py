import datetime
import time
import random_marker as rm
import infi.clickhouse_orm as ico
from random import randint


LOG2FIELD = {
    'dtm': lambda _: datetime.datetime.utcnow(),
    'dt': lambda _: datetime.datetime.utcnow(),
    'report_time': lambda _: datetime.datetime.utcnow(),
    'user_name': 'user',
    'user_domain': 'domain',
    'marker': 'marker',
    'department': 'department',
    'root_disk_serial': 'root_disk_serial',
    'ipv4_address': 'ip_addr',
    'hw_address': 'hw_addr'
}


DB_URL = 'http://10.11.20.98:8123'
CONNECTED_INT = 1
ROWS_NUM = 1


def gen_rows():
    rows = []
    for i in range(ROWS_NUM):
        row = LOG2FIELD
        row['user_name'] = rm.rand_user()
        row['user_domain'] = rm.rand_domain()
        row['marker'] = rm.rand_marker()
        row['department'] = rm.rand_department()
        row['root_disk_serial'] = rm.rand_disk()
        row['ipv4_address'] = rm.rand_ip()
        row['hw_address'] = rm.rand_hw()
        rows.append(row)
    return rows



class DMSpectator:

    BATCH_SIZE = 100
    LOGFILE = 'dmspectator_log'

    def connect(self):
        '''
        Соединение с DMIC.
        '''
        try:
            department_number = rm.rand_department()
            uname_ = f'department{department_number:05}'
            pass_ = f'pass{department_number:05}'

            self.db = ico.Database(
                'dmic',
                db_url=DB_URL,
                username=uname_,
                password=pass_)
            print("ОК")
            return True
        except Exception as ex_:
            print("Подключение не удалось")
        return False

    def process(self):
        self.connected = False
        while not self.connected:
            self.connected = self.connect()
            if not self.connected:
                time.sleep(CONNECTED_INT)
    
    def push_update(self):
        '''
        Массовая отправка блока записей.
        '''
        rows = gen_rows()
        print(rows) 
        try:
            self.db.insert(rows)
            return True
        except Exception as ex_:
            return False


def main():
    dms = DMSpectator()
    dms.process()
    print(dms.push_update())
    return 0


if __name__ == '__main__':
    main()
