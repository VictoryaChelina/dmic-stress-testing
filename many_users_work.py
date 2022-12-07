import datetime
import time
import os
import random_marker as rm
import infi.clickhouse_orm as ico
from infi.clickhouse_orm.fields import IPv4Field
from random import randint, random
from enum import Enum


DB_URL = 'http://10.11.20.98:8123'  # Адресс Dmic
CONNECTED_INT = 1  # Промежутки попыток подключения к БД
TIME_FACT = 1  # Промежутки между фактами маркирования на пользователе (в оригинале 10)
'''
Нужно ли вообще учитывать, что происходит на пользователе?

Задача перегрузить сервер и сделать это правдоподобно.

Строка на пользователе формируется каждые 10 секунд.
От одного пользователя выгрузка каждые 60 секунд. (По 6 строк, если подключен).
Если нет подключения, то накопленные логи пушатся по максимум 100 штук раз в минуту.
100 строк копятся 1000 секунд. 


МАКСИМАЛЬНАЯ ЗГРУЗКА - НАИХУДШИЙ СЦЕНАРИЙ
В любом случае, максимальная нагрузка от одного пользователя 100 строк в минуту.

Нужно дойти до 1 млн пользователей.
Значит в самом плохом случае 1 млн пользователей, попав в один временной промежуток, 
будут заваливать сервер 100 млн строк в минуту. 
Для этого у 1 млн пользователей должно накопиться хотя бы по 100 строк у каждого.
100 строк копятся, если нет возможности отправить лог 1000 секунд (почти 17 минут).


МИНИМАЛЬНАЯ ЗАГРУЗКА - НАИЛУЧШИЙ СЦЕНАРИЙ
У 1 млн пользователей постоянно отсылаются логи. Значит по 6 строчек от каждого в минуту.
Получается 6 млн строк в минуту от всех.
А в реальности даже лучше, т.к. нет каких-то общих часов,
по которым спектраторы на всех пользователях одновременно бы отправили свои 6 строк из логов. 
'''

ROWS_NUM = 5  # Количество генерируемых строк от одного пользователя
USERS_NUM = 1 # Количество пользователей
BATCH_SIZE = 5  # Количество строк отправляемых за одну загрузку (в оригинале 100)


#Operation = Enum('Operation', ['SCREEN', 'PRINT'])


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
    #operation_type = ico.Enum8Field(Operation)



# Генератор пользователей (пока пользователи генерируются поочереди)
def gen_users():
    users = []
    for i in range(USERS_NUM):
        user = gen_rows_one_user()
        users += user
    return users


# Генератор рандомных строк для таблиц screenmarkfact (один пользователь)
# report_time нужно изменить далее
# @profile
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
            hw_address=hw_address)
            #operation_type = Operation.SCREEN if random() < 0.95 else Operation.PRINT)
        rows_one_user.append(row)
        # time.sleep(TIME_FACT)
    return rows_one_user


def make_path():
    path = os.getcwd()
    path = os.mkdir(path + f'\\log\\{str(rm.rand_folder())}\\')
    return str(path)


# Генератор файла screenmark_log
def one_user_screen_log():
    path = make_path()
    with open(path + "screenmark_log", "w") as log:
        rows = gen_rows_one_user()
        for row in rows:
            log.write(
                f'{str(row.dtm)[:19]},{str(row.dtm)[20:23]} - event_api.py: INFO - screen-marking;' +
                f'{row.user_name};' + 
                f'{row.user_domain};' +
                f'{row.marker};' +
                f'{row.department};' +
                f'{row.root_disk_serial};' + 
                f'{row.ipv4_address};' +
                f'{row.hw_address}' + '\n')


# Генератор файла printmark_log
def one_user_print_log():
    path = make_path()
    with open(path + "screenmark_log", "w") as log:
        rows = gen_rows_one_user()
        for row in rows:
            log.write(
                f'{str(row.dtm)[:19]},{str(row.dtm)[20:23]} - event_api.py: INFO - print-marking;' + #  надо уточнить (посмотреть в printmark_log)
                f'{row.user_name};' + 
                f'{row.user_domain};' +
                f'{row.marker};' +
                f'{row.department};' +
                f'{row.root_disk_serial};' + 
                f'{row.ipv4_address};' +
                f'{row.hw_address}' + '\n')


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
    # @profile
    def push_update(self):
        rows = gen_users()
        print("Отправка логов")
        self.db.insert(rows, BATCH_SIZE)


def main():
    dms = DMSpectator()
    dms.process()
    t_start = time.time()
    print(dms.push_update())
    print(time.time() - t_start)
    # one_user_screen_log()
    # one_user_print_log()

    return 0


if __name__ == '__main__':
    main()
