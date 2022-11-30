import datetime
import time
import random_marker as rm
import infi.clickhouse_orm as ico
from infi.clickhouse_orm.fields import IPv4Field
from random import randint
import enum


DB_URL = 'http://10.11.20.98:8123'  # Адресс Dmic
CONNECTED_INT = 1  # Промежутки попыток подключения к БД
TIME_FACT = 1  # Промежутки между фактами маркирования (в оригинале 10)
ROWS_NUM = 50  # Количество генерируемых строк от одного пользователя
USERS_NUM = 2 # Количество пользователей
BATCH_SIZE = 4  # Количество строк отправляемых за одну загрузку (в оригинале 100)


class Operation(enum.Enum):
    SCREEN = 1
    PRINT = 2


# Модель таблиц
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
    operation_type = ico.Enum8Field(Operation)



# Генератор пользователей (пока пользователи генерируются поочереди)
def gen_users():
    users = []
    for i in range(USERS_NUM):
        user = gen_rows_one_user()
        users += user
    return users


# Генератор рандомных строк для таблиц screenmarkfact (один пользователь)
# report_time нужно изменить далее
def gen_rows_one_user():
    rows_one_user = []
    user_name = rm.rand_user()
    user_domain = rm.rand_domain()
    department = rm.rand_department()
    root_disk_serial = rm.rand_disk()
    marker = rm.rand_marker(department, root_disk_serial, user_name, user_domain)
    ipv4_address = rm.rand_ip()
    hw_address = rm.rand_hw()[:17]
    for i in range(ROWS_NUM):
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
            hw_address=hw_address,\
            operation_type = rm.rand_operation())
        rows_one_user.append(row)
        time.sleep(TIME_FACT)
    return rows_one_user


# Класс отправки псевдологов
class DMSpectator:
    LOGFILE = 'dmspectator_log'

    # Подключение к серверу
    def connect(self):
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
    
    # Отправка псевдологов
    def push_update(self):
        rows = gen_users()
        print("Отправка логов") 
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
