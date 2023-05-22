import datetime
import time
from time import perf_counter
import dmic_stress_testing.random_marker as rm
from dmic_stress_testing.script_parser import read_config
import infi.clickhouse_orm as ico
from dmic_stress_testing.p_database import p_db
from random import randint
import logging
from logging import FileHandler
import threading
import concurrent.futures
import csv
from tqdm import tqdm
import queue
from dmic_stress_testing.models import screenmarkfact, markfact
from random import shuffle


THREAD = True
POOL = True

# Класс отправки псевдологов
class SpectatorTesting:
    def __init__(self, configuration):
        self.configuration = configuration
        if self.configuration['LOG'] is not None:
            self.f = open('./' + self.configuration['LOG'], 'w', newline='')
            self.writer = csv.writer(self.f, delimiter=',', quotechar='|')

    # Словарь id: ico.Database (экземпляры подключения)
    connections = {}

    # Список пользователей, которые не смогли
    # подключиться к базе за максимальное число попыток
    not_connected_users = []
    push_q = queue.Queue()

    # Словарь id: rm.RandUser
    users = {}

    # Словарь id: ScreenmarkFact подготовленная строка
    # для пушинга (неизменяемая часть)
    rows_const_part = {}

    # Словарь id: время последней отправки с пользователя
    last_push_time = {}

    # Словарь id: всего строк отправлено от пользователя
    user_rows_count = {}

    # Сбор метрик
    rows_per_second = []
    total_user_push = 0
    insertion_time = 0
    start_connection_time = None
    stop_connection_time = None
    start_insertion_time = None
    last_insertion_time = None

    stop_threading = False


    def db_scheme(self, id, user):
        if self.configuration['DB_SCHEME'] == 'changed':
            self.rows_const_part[id] = markfact(
                    dt=datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    dtm=datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    report_time=datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    user_name=user.user_name,
                    user_domain=user.user_domain,
                    marker=user.marker,
                    department=user.department,
                    root_disk_serial=user.disk,
                    ipv4_address=user.ip,
                    hw_address=user.hw,
                    operation_type=1)
        else:
            self.rows_const_part[id] = screenmarkfact(
                    dt=datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    dtm=datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    report_time=datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    user_name=user.user_name,
                    user_domain=user.user_domain,
                    marker=user.marker,
                    department=user.department,
                    root_disk_serial=user.disk,
                    ipv4_address=user.ip,
                    hw_address=user.hw)


    # Генерируется заданное число пользователей
    def gen_users(self):
        for department in range(self.configuration['DEPARTMENT_NUM']):
            department = randint(1, 65355)
            print(department)
            for user in range(self.configuration['USERS_NUM']):
                user = rm.RandUser(department=department)
                id = user.user_id()
                self.users[id] = user
                self.connections[id] = None
                self.last_push_time[id] = (
                    datetime.datetime.today()
                    - datetime.timedelta(minutes=1))
                self.user_rows_count[id] = 0
                # user.user_info()
                self.db_scheme(id, user)
        self.pbar = tqdm(total=len(self.users), desc='Connecting users')

    # Подключение к базе
    def connect(self, id):
        try:
            user = self.users[id]
            department_number = int(user.department)
            uname_ = f'department{department_number:05}'
            pass_ = f'pass{department_number:05}'

            self.db = p_db(
                'dmic',
                db_url=self.configuration['DB_URL'],
                username=uname_,
                password=pass_,
                source_ip=self.configuration['SOURCE_IP'],
                timeout=300)
            
            self.connections[id] = self.db
            return True
        except Exception:
            pass
        return False

    def process(self, id):
        self.connected = False
        attempts_counter = 0
        while (not self.connected and
                attempts_counter <=
                self.configuration['MAX_CONNECTION_ATTEMPTS']):
            attempts_counter += 1
            self.connected = self.connect(id=id)
            if not self.connected:
                time.sleep(self.configuration['CONNECTION_INTERVAL'])
        if not self.connected:
            self.not_connected_users.append(id)
        else:
            self.pbar.update(1)

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

        # Время записи в лог на клиенте факта маркирования
        mark_time = report_time

        # Интервал между фактами маркирования
        delta = datetime.timedelta(seconds=self.configuration['MARK_INTERVAL'])
        row = self.rows_const_part[id]
        for i in range(self.configuration['ROWS_NUM'], 0, -1):
            mark_time = report_time - delta * i
            row.dt = mark_time - datetime.timedelta(days=self.configuration['D_ROWS'])
            row.dtm = mark_time
            row.report_time = report_time
            rows.append(row)
        return rows

    def insertion(self, id, rows):
        try:
            self.connections[id].insert(rows, self.configuration['ROWS_NUM'])
            self.last_insertion_time = perf_counter()
            self.total_user_push += self.configuration['ROWS_NUM']
            self.user_rows_count[id] += self.configuration['ROWS_NUM']
            time_from_start = (
                self.last_insertion_time
                - self.start_insertion_time)
            rps = self.total_user_push / time_from_start
            self.rows_per_second.append(rps)
            
            if self.configuration["LOG"] is not None:
                self.writer.writerow([time_from_start, self.total_user_push, rps])
            self.pbar.update(1)
        except Exception as ex:
            exception_time = datetime.datetime.today()
            print(f'{exception_time} Exeption "{ex}" accured while pushing rows')
            print(f'Переподключение {id}')
            self.process(id)  # если ошибка, пробуем переподключить
            self.stop_threading = True

    def push_update_one_user(self, id):
        # Время отправки строк лога с клиента на dmic
        report_time = datetime.datetime.today()
        time_pass = (report_time - self.last_push_time[id]
                >= datetime.timedelta(seconds=self.configuration['PUSH_INT']))
        if time_pass:
            rows = self.gen_rows(id=id, report_time=report_time)
            self.user_rows_count[id] += self.configuration['ROWS_NUM']
            if THREAD:
                threading.Thread(
                    target=self.insertion,
                    args=(id, rows),
                    daemon=True).start()
            else:
                self.insertion(id=id, rows=rows)
            self.last_push_time[id] = report_time
        else: 
            self.push_q.put(id)

    # Запускает цикл по connections для отправки логов
    def loops(self):
        self.pbar = tqdm(
            total=len(self.users) * self.configuration['AMOUNT'],
            desc='Inserting rows')
        for _ in range(self.configuration['AMOUNT']):
            ids = list(self.connections.keys())
            shuffle(ids)
            for id in ids:
                if self.stop_threading:
                    return
                if not self.connections[id]:
                    continue
                while threading.active_count() > self.configuration["LIMIT"]:
                    continue
                self.push_q.put(id)
        self.implement_q()
    
    def implement_q(self):
        while not self.push_q.empty():
            self.push_update_one_user(id=self.push_q.get())

    def interval(self):
        self.pbar = tqdm(
            total=len(self.users) * self.configuration['AMOUNT'],
            desc='Inserting rows')
        start_interval = datetime.datetime.today()
        amount = self.configuration['AMOUNT']
        delta = datetime.timedelta(seconds=amount)
        while datetime.datetime.today() - start_interval < delta:
            for id in self.users.keys():
                if self.stop_threading:
                    return
                while threading.active_count() > self.configuration["LIMIT"]:
                    continue
                self.push_update_one_user(id=id)

    def metrics(self):
        average_rps = self.total_user_push /\
            (self.last_insertion_time - self.start_insertion_time)

        padding = 40
        print('Metrics:\n')
        print(
            'LIMIT:'.ljust(padding),
            self.configuration['LIMIT'])
        print(
            'Number of departments:'.ljust(padding),
            self.configuration['DEPARTMENT_NUM'])
        print(
            'Number of users per department:'.ljust(padding),
            self.configuration['USERS_NUM'])
        print(
            'Total number of users:'.ljust(padding),
            self.configuration['USERS_NUM']
            * self.configuration['DEPARTMENT_NUM'])
        print(
            'Total number of rows:'.ljust(padding),
            self.total_user_push, '\n')
        print(
            'Connecting time (s):'.ljust(padding),
            self.stop_connection_time - self.start_connection_time, '\n')
        print(
            'Average rps:'.ljust(padding),
            average_rps, '\n')
        print(
            'Test ended at:'.ljust(padding),
            datetime.datetime.today(), '\n')

    def entr_point(self):
        self.gen_users()
        self.start_connection_time = perf_counter()
        self.connect_users() 
        self.stop_connection_time = perf_counter()
        while threading.active_count() > 2:
            continue
        self.pbar.close()

        self.start_insertion_time = perf_counter()
        try:
            if self.configuration['INTERVAL'] == 'loops':
                self.loops()
            else:
                self.interval()
        except KeyboardInterrupt:
            print("Interruption")
        finally:
            self.last_insertion_time = perf_counter()
            while threading.active_count() > 2:
                continue
            self.pbar.close()
            self.metrics()


def main():
    print(
        'Test started at:'.ljust(40),
        datetime.datetime.today(), '\n')
    configuration = read_config()
    test = SpectatorTesting(configuration=configuration)
    test.entr_point()
    return 0


def main_main(configuration):
    print(
        'Test started at:'.ljust(40),
        datetime.datetime.today(), '\n')
    test = SpectatorTesting(configuration=configuration)
    test.entr_point()
    return 0


if __name__ == '__main__':
    main()
