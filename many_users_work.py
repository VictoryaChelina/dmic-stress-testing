import datetime
import random_marker as rm
import infi.clickhouse_orm as ico


LOG2FIELD = {
    'dtm': 'timestamp',
    'dt': lambda row: row.timestamp[:10],
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
            return True
        except Exception as ex_:
            print("Подключение не удалось")
        return False

def main():
    dms = DMSpectator()
    print(dms.connect())
    return 0


if __name__ == '__main__':
    main()
