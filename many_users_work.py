import datetime
import time
import random_marker as rm
import infi.clickhouse_orm as ico
#from infi.clickhouse_orm.fields import IPv4Field
from random import randint


DB_URL = 'http://10.11.20.98:8123'
CONNECTED_INT = 10
ROWS_NUM = 1000
BATCH_SIZE = 100


class ScreenmarkFact(ico.Model):
    dt = ico.DateField()
    dtm = ico.DateTimeField()
    report_time = ico.DateTimeField()
    user_name = ico.StringField()
    user_domain = ico.StringField()
    marker = ico.StringField()
    department = ico.UInt32Field()
    root_disk_serial = ico.StringField()
    ipv4_address = ico.IPv4Field()
    hw_address = ico.FixedStringField(length=17)


def gen_rows():
    rows = []
    for i in range(ROWS_NUM):
        user_name = rm.rand_user()
        user_domain = rm.rand_domain()
        department = rm.rand_department()
        root_disk_serial = rm.rand_disk()
        marker = rm.rand_marker(department, root_disk_serial, user_name, user_domain)
        ipv4_address = rm.rand_ip()
        hw_address = rm.rand_hw()[:17]
        row = ScreenmarkFact(
            dt = datetime.date.today(),\
            dtm = datetime.datetime.today(),\
            report_time = datetime.datetime.today(),\
            user_name=user_name,\
            user_domain=user_domain,\
            department=department,\
            root_disk_serial=root_disk_serial,\
            marker=marker,\
            ipv4_address=ipv4_address,\
            hw_address=hw_address)
        rows.append(row)
    return rows


class DMSpectator:
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
            rc = self.db.insert(rows, BATCH_SIZE)
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
