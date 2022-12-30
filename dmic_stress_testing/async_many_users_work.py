import datetime
from time import perf_counter
import dmic_stress_testing.random_marker as rm
from dmic_stress_testing.common import read_config
from random import randint
import logging
import numpy as np
import csv

import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
 

logging.basicConfig(level=logging.DEBUG)


# Модель таблиц
class Row():
    def __init__(
        self,
        user_name,
        user_domain,
        marker, 
        department,
        root_disk_serial,
        ipv4_address,
        hw_address):

        dt = None
        dtm = None
        report_time = None
        user_name = user_name
        user_domain = user_domain
        marker = marker
        department = department
        root_disk_serial = root_disk_serial
        ipv4_address = ipv4_address
        hw_address = hw_address
        #operation_type = ico.Enum8Field(Operation)
        

# Класс отправки псевдологов
class SpectatorTesting:
    def __init__(self, configuration):
        self.configuration = configuration
    connections = {}  # Словарь id: ico.Database (экземпляры подключения)
    not_connected_users = []  # Список пользователей, которые не смогли подключиться к базе за максимальное число попыток
    users = {}  # Словарь id: rm.RandUser 
    rows_const_part = {}  # Словарь id: ScreenmarkFact подготовленная строка для пушинга (неизменяемая часть)
    last_push_time = {}  # Словарь id: время последней отправки с пользователя
    user_rows_count = {}  # Словарь id: всего строк отправлено от пользователя

    # Сбор метрик
    row_generation_time = []  # учитывается время добавления к заготовленной строке временных значений 
    user_connection_time = []  # учитывает время подключения одного пользователя (если в итоге подключился)
    row_insertion_time = []  # учитывает время вставки строк в базу
    rows_per_second = []  # Строк в секунду (при каждом добавлении от пользователя)
    client_sessions = []
    total_user_connection = 0
    total_user_push = 0
    insertion_time = 0
    last_push_pc = None
    start_time = None
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
                self.rows_const_part[id] = [
                    datetime.datetime.today(),
                    datetime.datetime.today(),
                    datetime.datetime.today(),
                    user.user_name, 
                    user.user_domain,
                    user.marker,
                    user.department,
                    user.disk,
                    user.ip,
                    user.hw]
    
    # Вставка строки
    async def insert_rows_one_user(self, id):
        report_time = datetime.datetime.today()
        mark_time = report_time
        self.rows_const_part[id][2] = report_time
        client = self.connections[id]
        delta = datetime.timedelta(seconds=self.configuration['MARK_INTERVAL'])
        rows = []
        for i in range(self.configuration['ROWS_NUM'], 0, -1):
            start = perf_counter()
            mark_time = report_time - delta * i
            self.rows_const_part[id][0] = mark_time
            self.rows_const_part[id][1] = mark_time
            row = self.rows_const_part[id]
            stop = perf_counter()
            self.row_generation_time.append(stop-start)
            rows.append(row)
        start = perf_counter()
        await client.execute("INSERT INTO screenmarkfact (*) VALUES", *rows)
        stop = perf_counter()
        self.last_insertion_time = stop
        self.last_push_pc = perf_counter()
        self.row_insertion_time.append((stop-start)/len(rows))
        self.total_user_push += self.configuration['ROWS_NUM']
        #logging.info(f'client with id {id} insert rows')
        rps = self.total_user_push / datetime.datetime.timestamp(self.start_insertion_time)
        self.rows_per_second.append(rps)
        logging.info(f'rps {rps} insert rows')
        print(f'rps: {rps}', end='\r')
        with open('./some.csv', 'w', newline='') as f:
            writer = csv.writer(f, delimiter=',', quotechar='|')
            writer.writerow([self.last_insertion_time, self.total_user_push])

    async def insert_rows_many_users(self):
        for id in self.connections:
            if len(asyncio.all_tasks(asyncio.get_running_loop())) < self.configuration["ASYNC_LIMIT"] and \
            datetime.datetime.today() - self.last_push_time[id] >= datetime.timedelta(seconds=self.configuration['PUSH_INT']):
                logging.debug(f'{id} insert rows')
                asyncio.create_task(self.insert_rows_one_user(id))
                self.last_push_time[id] = datetime.datetime.today()
            else:
                await asyncio.sleep(0)

    async def timeless(self):
        while True:
            await self.insert_rows_many_users()

    async def interval(self):
        start_interval = datetime.datetime.today()
        delta = datetime.timedelta(minutes=int(self.configuration['INTERVAL']))
        while (datetime.datetime.today() - start_interval < delta):
            await self.insert_rows_many_users()

    # Подключение клиента 
    async def connect_client(self, id):
        start = perf_counter()
        s = ClientSession()
        self.client_sessions.append(s)
        user = self.users[id]
        department_number = int(user.department)
        uname_ = f'department{department_number:05}'
        pass_ = f'pass{department_number:05}'
        client = ChClient(
            session=s,
            url=self.configuration['DB_URL'],
            database='dmic',
            user=uname_,
            password=pass_)
        self.connections[id] = client
        stop = perf_counter()
        self.user_connection_time.append(stop - start)
        self.total_user_connection += 1
        logging.info(f'client with id {id} connected')
    
    # Цикл по клиентам 
    async def connect_clients(self):
        futures = [self.connect_client(id=id) for id in self.users.keys()]
        await asyncio.gather(*futures)
        logging.info(f'clients done')

    async def close_connections(self):
        cnt = 0
        for id in self.connections:
            client = self.connections[id]
            await client.close()
            cnt += 1
            logging.debug(f'all connections{len(self.connections)}, closed{cnt}')
    
    async def close_client_session(self):
        cnt = 0
        for s in self.client_sessions:
            await s.close()
            cnt += 1
            logging.debug(f'all sessions{len(self.connections)}, closed{cnt}')

    def metrics(self):
        row_generation = np.array(self.row_generation_time)
        average_row_generation = np.average(row_generation)
        rows_num = row_generation.size
        total_row_generation = np.sum(row_generation)
        rps = np.array(self.rows_per_second)
        average_rps = np.average(rps)

        user_connection = np.array(self.user_connection_time)
        average_user_connection = np.average(user_connection)
        connections_num = user_connection.size

        row_insertion = np.array(self.row_insertion_time)
        average_row_insertion = np.average(row_insertion)

        padding = 40
        print('МЕТРИКИ:\n')
        print('ASYNC_LIMIT:'.ljust(padding), self.configuration['ASYNC_LIMIT'])

        print('Number of departments:'.ljust(padding), self.configuration['DEPARTMENT_NUM'])
        print('Number of users per department:'.ljust(padding), self.configuration['USERS_NUM'])
        print('Total number of users:'.ljust(padding), self.configuration['USERS_NUM'] * self.configuration['DEPARTMENT_NUM'], '\n')

        print('Средний rps:'.ljust(padding), average_rps, '\n')
        print('Всего времени потрачено:'.ljust(padding), self.insertion_time, '\n')
        print('Время окончания теста:'.ljust(padding), datetime.datetime.today(), '\n')

    async def entr_point(self):
        self.gen_users()
        await self.connect_clients()
        if self.configuration['INTERVAL'] == 'timeless':
            self.start_insertion_time = datetime.datetime.now()
            logging.debug(f'start timeless')
            await self.timeless()
        else:
            self.start_insertion_time = datetime.datetime.today()
            logging.debug(f'start interval')
            await self.interval()
        print('Closing connections')
        while len(asyncio.all_tasks(asyncio.get_running_loop())) > 1:
            await asyncio.sleep(0)
        await self.close_connections()
        #await self.close_client_session()
        self.metrics()


async def main():
    configuration = read_config()
    start_test = perf_counter()
    test = SpectatorTesting(configuration=configuration)
    test.start_time = perf_counter()
    test.last_push_pc = perf_counter()
    await test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return

async def main_main(configuration):
    start_test = perf_counter()
    test = SpectatorTesting(configuration=configuration)
    test.start_time = perf_counter()
    test.last_push_pc = perf_counter()
    await test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return 0

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print('KB interrupt')


