import datetime
import time
from time import perf_counter
import dmic_stress_testing.random_marker as rm
from dmic_stress_testing.common import read_config
import infi.clickhouse_orm as ico
from random import randint, random
from enum import Enum
import traceback
import logging
import threading
import concurrent.futures
import numpy as np
import argparse
import json 
import csv


THREAD = True
POOL = True

logging.basicConfig(level=logging.WARNING)


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
    def __init__(self, configuration):
        self.configuration = configuration
        self.f = open('./' + self.configuration['LOG'], 'w', newline='')
    connections = {}  # Словарь id: ico.Database (экземпляры подключения)
    not_connected_users = []  # Список пользователей, которые не смогли подключиться к базе за максимальное число попыток
    users = {}  # Словарь id: rm.RandUser 
    rows_const_part = {}  # Словарь id: ScreenmarkFact подготовленная строка для пушинга (неизменяемая часть)
    last_push_time = {}  # Словарь id: время последней отправки с пользователя
    user_rows_count = {}  # Словарь id: всего строк отправлено от пользователя

    # Сбор метрик
    rows_per_second = []  # Строк в секунду (при каждом добавлении от пользователя)
    total_user_push = 0
    insertion_time = 0
    start_connection_time = None
    stop_connection_time = None
    start_insertion_time = None
    last_insertion_time = None


    # Генерируется заданное число пользователей
    def gen_users(self):
        for department in range(self.configuration['DEPARTMENT_NUM']):
            department = randint(1, 65535)
            for user in range(self.configuration['USERS_NUM']):
                user = rm.RandUser(department=department)
                id = user.user_id()
                self.users[id] = user
                self.connections[id] = None
                self.last_push_time[id] = datetime.datetime.today() - datetime.timedelta(minutes=1)
                self.user_rows_count[id] = 0
                # user.user_info()
                self.rows_const_part[id] = ScreenmarkFact(
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
            user = self.users[id]
            department_number = int(user.department)
            uname_ = f'department{department_number:05}'
            pass_ = f'pass{department_number:05}'

            self.db = ico.Database(
                'dmic',
                db_url=self.configuration['DB_URL'],
                username=uname_,
                password=pass_)
            self.connections[id] = self.db
            logging.info(f'{id} {uname_} {pass_}: Подключился базе')
            return True
        except Exception as ex_:
            logging.info(f'{id} {uname_} {pass_}: Подключение...')
            #traceback.print_exc()
        return False

    def process(self, id):
        self.connected = False
        attempts_counter = 0
        while not self.connected and attempts_counter <= self.configuration['MAX_CONNECTION_ATTEMPTS']:
            attempts_counter += 1
            self.connected = self.connect(id=id)
            if not self.connected:
                time.sleep(self.configuration['CONNECTION_INTERVAL'])
        if self.connected == False:
            self.not_connected_users.append(id)

    # Пользователи подключаются к базе
    def connect_users(self):
        if POOL:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                executor.map(self.process, self.users.keys())
        else:
            for id in self.users.keys():
                self.process(id=id)

    # Генерация строк
    def gen_rows(self, id, report_time):
        rows = []
        mark_time = report_time  # Время записи в лог на клиенте факта маркирования
        delta = datetime.timedelta(seconds=self.configuration['MARK_INTERVAL'])  # Интервал между фактами маркирования 
        row = self.rows_const_part[id]  # Подготовленная строка по данному пользователю
        for i in range(self.configuration['ROWS_NUM'], 0, -1):
            mark_time = report_time - delta * i  # Меняется время маркирования для записи строки
            row.dt = mark_time  # В подготовленную строку добавляются отметки времени
            row.dtm = mark_time
            row.report_time = report_time
            rows.append(row)
        return rows

    def insertion(self, id, rows):
        self.connections[id].insert(rows, self.configuration['BATCH_SIZE'])
        
        self.last_insertion_time = perf_counter()
        self.total_user_push += self.configuration['ROWS_NUM']
        self.user_rows_count[id] += self.configuration['ROWS_NUM']
        time_from_start = self.last_insertion_time - self.start_insertion_time
        rps = self.total_user_push / time_from_start
        self.rows_per_second.append(rps)

        print(f'rps: {rps}', end='\r')
        writer = csv.writer(self.f, delimiter=',', quotechar='|')
        writer.writerow([time_from_start, self.total_user_push, rps])
    
    def push_update_one_user(self, id):
        report_time = datetime.datetime.today()  # Время отправки строк лога с клиента на dmic
        if report_time - self.last_push_time[id] >= datetime.timedelta(seconds=self.configuration['PUSH_INT']):
            rows = self.gen_rows(id = id, report_time=report_time)
            self.user_rows_count[id] += self.configuration['ROWS_NUM']
            if THREAD:
                threading.Thread(target=self.insertion, args=(id, rows)).start()
            else:
                self.insertion(id=id, rows=rows)
            self.last_push_time[id] = report_time

    # Запускает цикл по connections для отправки логов
    def timeless(self):
        while True:
            for id in self.users.keys():
                self.push_update_one_user(id=id)

    def interval(self):
        start_interval = datetime.datetime.today()
        mesurment = self.configuration['INTERVAL'][1]
        amount = int(self.configuration['INTERVAL'][0])
        if mesurment == 's':
            delta = datetime.timedelta(seconds=amount)
        elif mesurment == 'm':
            delta = datetime.timedelta(minutes=amount)
        else:
            delta = datetime.timedelta(hours=amount)
        while (datetime.datetime.today() - start_interval < delta):
            for id in self.users.keys():
                self.push_update_one_user(id=id)
    
    def metrics(self):
        rps = np.array(self.rows_per_second)
        average_rps = np.average(rps)

        padding = 40
        print('МЕТРИКИ:\n')
        
        print(':', 'ASYNC_LIMIT:'.ljust(padding), 'This is thread mode')
        print(':', 'Number of departments:'.ljust(padding), self.configuration['DEPARTMENT_NUM'])
        print(':', 'Number of users per department:'.ljust(padding), self.configuration['USERS_NUM'])
        print(':', 'Total number of users:'.ljust(padding), self.configuration['USERS_NUM'] * self.configuration['DEPARTMENT_NUM'])
        print(':', 'Total number of rows:'.ljust(padding), self.total_user_push, '\n')

        print(':', 'Время на подключение:'.ljust(padding), self.stop_connection_time - self.start_connection_time, '\n')

        print(':', 'Средний rps:'.ljust(padding), average_rps, '\n')
        print(':', 'Время окончания теста:'.ljust(padding), datetime.datetime.today(), '\n')

    def entr_point(self):
        start_gen = perf_counter()
        self.gen_users()  # Создаются пользователи, подключения и подготавливаются неизменяемые части строк
        end_gen = perf_counter()
        logging.warning(f'Generated users {len(self.users)} in {end_gen-start_gen} seconds')
        self.start_connection_time = perf_counter()
        self.connect_users()  # Пользователи подключаются к базе
        self.stop_connection_time = perf_counter()
        end_connect = perf_counter()
        logging.warning(f'Connected users {len(self.users)} in {end_connect-end_gen} seconds')
        self.start_insertion_time = perf_counter()
        try:
            if self.configuration['INTERVAL'][0] == 'timeless':
                logging.debug(f'start timeless')
                self.timeless()  # В бесконечном цикле пушатся строки от пользователей
            else:
                logging.debug(f'start interval')
                self.interval()
        except KeyboardInterrupt:
            print("Interruption")
            end_push = perf_counter()
            logging.warning(f'pushed rows in {end_push-end_connect} seconds')
            self.metrics()
        else:
            end_push = perf_counter()
            logging.warning(f'pushed rows in {end_push-end_connect} seconds')
            self.metrics()


def result_config(config):
    with open('config.json') as defualt_config:
        default = json.load(defualt_config)
    with open(config) as alter_config:
        alter = json.load(alter_config)
    if default == alter:
        return alter
    for key in default.keys():
        if key not in alter.keys():
            alter[key] = default[key]
    return alter


def main():
    configuration = read_config()
    start_test = perf_counter()
    test = SpectatorTesting(configuration=configuration)
    test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return 0

def main_main(configuration):
    start_test = perf_counter()
    test = SpectatorTesting(configuration=configuration)
    test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return 0

if __name__ == '__main__':
    main()