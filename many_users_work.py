import datetime
import time
from time import perf_counter
import os
import random_marker_copy as rm
import infi.clickhouse_orm as ico
from random import randint, random
from enum import Enum
import traceback
import logging


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


DB_URL = 'http://10.11.20.98:8123'  # Адресс Dmic
CONNECTED_INT = 1  # Промежутки попыток подключения к БД (в секундах)
ROWS_NUM = 2  # Количество генерируемых строк от одного пользователя в минуту
USERS_NUM = 2 # Количество пользователей
BATCH_SIZE = 100  # Количество строк отправляемых за одну загрузку (в оригинале 100)
PUSH_INT = 60  # Время между отправкой update от пользователя в базу (в секундах)
MARK_INT = 10  # Промежутки между фактами маркирования на пользователе (в секнудах)
LOG_LEVEL = 10


logging.basicConfig(level=logging.WARNING)
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

    def row_info(self):
        padding = 17
        print('USER INFORMATION:')
        print('dt:'.ljust(padding), self.dt)
        print('dtm:'.ljust(padding), self.dtm)
        print('report_time:'.ljust(padding), self.report_time)
        print('user_name:'.ljust(padding), self.user_name)
        print('user_domain:'.ljust(padding), self.user_domain)
        print('marker:'.ljust(padding), self.marker)
        print('department:'.ljust(padding), self.department)
        print('root_disk_serial:'.ljust(padding), self.root_disk_serial)
        print('ipv4_address:'.ljust(padding), self.ipv4_address)
        print('hw_address:'.ljust(padding), self.hw_address)
        

# Класс отправки псевдологов
class SpectatorTesting:

    CONNECTIONS = {}  # Словарь id: ico.Database (экземпляры подключения)
    USERS = {}  # Словарь id: rm.RandUser 
    ROWS = {}  # Словарь id: ScreenmarkFact подготовленная строка для пушинга (неизменяемая часть)
    LAST_PUSH_TIME = {}  # Словарь id: время последней отправки с пользователя
    USER_ROWS_COUNT = {}  # Словарь id: всего строк отправлено от пользователя

    #start_time = None
    #stop_time = None

    # Генерируется заданное число пользователей
    def gen_users(self):
        for user in range(USERS_NUM):
            user = rm.RandUser()
            self.USERS[user.user_id()] = user
            self.CONNECTIONS[user.user_id()] = None
            self.LAST_PUSH_TIME[user.user_id()] = datetime.datetime.today() - datetime.timedelta(minutes=1)
            self.USER_ROWS_COUNT[user.user_id()] = 0
            # user.user_info()
            self.ROWS[user.user_id()] = ScreenmarkFact(
                dt=datetime.datetime(1984, 1, 1, 1, 1, 1, 1), \
                dtm=datetime.datetime(1984, 1, 1, 1, 1, 1, 1), \
                report_time=datetime.datetime(1984, 1, 1, 1, 1, 1, 1), \
                user_name=user.user_name, \
                user_domain=user.user_domain, \
                marker=user.marker, \
                department=user.department, \
                root_disk_serial=user.disk, \
                ipv4_address=user.ip, \
                hw_address=user.hw)

    # Подключение к базе
    def connect(self, id):
        try:
            user = self.USERS[id]
            department_number = int(user.department)
            uname_ = f'department{department_number:05}'
            pass_ = f'pass{department_number:05}'

            self.db = ico.Database(
                'dmic',
                db_url=DB_URL,
                username=uname_,
                password=pass_)
            self.CONNECTIONS[id] = self.db
            logging.info(f'{id} {uname_} {pass_}: Подключился базе')
            return True
        except Exception as ex_:
            logging.info(f'{id} {uname_} {pass_}: Подключение...')
            #traceback.print_exc()
        return False

    def process(self, id):
        self.connected = False
        while not self.connected:
            self.connected = self.connect(id=id)
            if not self.connected:
                time.sleep(CONNECTED_INT)

    # Пользователи подключаются к базе
    def connect_users(self):
        for id in self.USERS.keys():
            self.process(id=id)

    # Генерация строк
    def gen_rows(self, id, report_time):
        rows = []
        mark_time = report_time  # Время записи в лог на клиенте факта маркирования
        delta = datetime.timedelta(seconds=MARK_INT)  # Интервал между фактами маркирования 
        row = self.ROWS[id]  # Подготовленная строка по данному пользователю
        for i in range(ROWS_NUM, 0, -1):
            mark_time = report_time - delta * i  # Меняется время маркирования для записи строки
            row.dt = mark_time  # В подготовленную строку добавляются отметки времени
            row.dtm = mark_time
            row.report_time = report_time
            rows.append(row)
        return rows
        
    # Запускает цикл по CONNECTIONS для отправки логов
    def pushing_updates(self):
        start_time = datetime.datetime.today()  # Время начала загрузки (необходимо для проверки)
        while True:
            for id in self.CONNECTIONS.keys():
                report_time = datetime.datetime.today()  # Время отправки строк лога с клиента на dmic
                if report_time - self.LAST_PUSH_TIME[id] < datetime.timedelta(seconds=PUSH_INT):
                    continue
                else:
                    rows = self.gen_rows(id = id, report_time=report_time)
                    # rows = []
                    # mark_time = report_time  # Время записи в лог на клиенте факта маркирования
                    # delta = datetime.timedelta(seconds=MARK_INT)  # Интервал между фактами маркирования 
                    # row = self.ROWS[id]  # Подготовленная строка по данному пользователю
                    # for i in range(ROWS_NUM, 0, -1):
                    #     mark_time = report_time - delta * i  # Меняется время маркирования для записи строки
                    #     row.dt = mark_time  # В подготовленную строку добавляются отметки времени
                    #     row.dtm = mark_time
                    #     row.report_time = report_time
                    #     rows.append(row)
                    self.USER_ROWS_COUNT[id] += ROWS_NUM
                    start = perf_counter()
                    self.CONNECTIONS[id].insert(rows, BATCH_SIZE)
                    stop = perf_counter()
                    logging.debug(f'Insert time:{stop-start}')
                    self.LAST_PUSH_TIME[id] = report_time
            break
        stop_time = datetime.datetime.today()  # Время конца загрузки (необходимо для проверки)

    # Проверка, что строки действительно попали в базу
    ''' Подсчет, что за промежуток времени отправки от 
    каждого пользователя ушло строк столько,
     сколько соответсвует в словаре USER_EOWS_COUNT
    '''
    def check(self):
        connection = self.CONNECTIONS[0]
        statment = "SELECT * FROM dmic.user \
            WHERE "
        for user in connection.select(statment, model_class=ScreenmarkFact):
            print(user.marker)
        
        #for person in db.select("SELECT * FROM my_test_db.person", model_class=Person):
            #print(person.first_name, person.last_name)

    def entr_point(self):
        start_gen = perf_counter()
        self.gen_users()  # Создаются пользователи, подключения и подготавливаются неизменяемые части строк
        end_gen = perf_counter()
        logging.warning(f'Generated users {len(self.USERS)} in {end_gen-start_gen} seconds')
        self.connect_users()  # Пользователи подключаются к базе
        end_connect = perf_counter()
        logging.warning(f'Connected users {len(self.USERS)} in {end_connect-end_gen} seconds')
        self.pushing_updates()  # В бесконечном цикле пушатся строки от пользователей
        end_push = perf_counter()
        logging.warning(f'pushed rows in {end_push-end_connect} seconds')




def main():
    test = SpectatorTesting()
    test.entr_point()
    return 0


if __name__ == '__main__':
    main()