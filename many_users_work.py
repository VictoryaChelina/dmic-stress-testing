import datetime
import time
import random_marker as rm
import infi.clickhouse_orm as ico
from random import randint


DB_URL = 'http://10.11.20.98:8123'
CONNECTED_INT = 1
ROWS_NUM = 1


class Row(ico.Model):
    user_name = ico.StringField()
    user_domain = ico.StringField()
    marker = ico.StringField()
    department = ico.StringField()
    root_disk_serial = ico.StringField()
    ipv4_address = ico.StringField()
    hw_address = ico.StringField()


def gen_rows():
    rows = []
    for i in range(ROWS_NUM):
        user_name = rm.rand_user()
        user_domain = rm.rand_domain()
        department = rm.rand_domain()
        root_disk_serial = rm.rand_disk()
        marker = rm.rand_marker(department, root_disk_serial, user_name, user_domain)
        ipv4_address = rm.rand_ip()
        hw_address = rm.rand_hw()
        row = Row(user_name=user_name, user_domain=user_domain,\
             department=department, root_disk_serial=root_disk_serial,\
                 marker=marker, ipv4_address=ipv4_address, hw_address=hw_address)
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
