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
 

logging.basicConfig(level=logging.WARNING)


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
        self.f = open('./some.csv', 'w', newline='')
    connections = {}  # Словарь id: ico.Database (экземпляры подключения)
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
            mark_time = report_time - delta * i
            self.rows_const_part[id][0] = mark_time
            self.rows_const_part[id][1] = mark_time
            row = self.rows_const_part[id]
            rows.append(row)
        await client.execute("INSERT INTO screenmarkfact (*) VALUES", *rows)
        self.last_insertion_time = perf_counter()
        self.total_user_push += self.configuration['ROWS_NUM']
        self.user_rows_count[id] += self.configuration['ROWS_NUM']
        time_from_start = self.last_insertion_time - self.start_insertion_time
        rps = self.total_user_push / time_from_start
        self.rows_per_second.append(rps)
        logging.info(f'rps {rps}')
        print(f'rps: {rps}', end='\r')
        writer = csv.writer(self.f, delimiter=',', quotechar='|')
        writer.writerow([time_from_start, self.total_user_push, rps])

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
        s = ClientSession()
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
    
    def interruption_close_connections(self):
        cnt = 0
        for id in self.connections:
            client = self.connections[id]
            asyncio.run(client.close())
            cnt += 1
            logging.debug(f'all connections{len(self.connections)}, closed{cnt}')

    def metrics(self):
        rps = np.array(self.rows_per_second)
        average_rps = np.average(rps)

        padding = 40
        print('МЕТРИКИ:\n')
        
        print('ASYNC_LIMIT:'.ljust(padding), self.configuration['ASYNC_LIMIT'])
        print('Number of departments:'.ljust(padding), self.configuration['DEPARTMENT_NUM'])
        print('Number of users per department:'.ljust(padding), self.configuration['USERS_NUM'])
        print('Total number of users:'.ljust(padding), self.configuration['USERS_NUM'] * self.configuration['DEPARTMENT_NUM'])
        print('Total number of rows:'.ljust(padding), self.total_user_push, '\n')

        print('Время на подключение:'.ljust(padding), self.stop_connection_time - self.start_connection_time, '\n')

        print('Средний rps:'.ljust(padding), average_rps, '\n')
        print('Время окончания теста:'.ljust(padding), datetime.datetime.today(), '\n')

    async def entr_point(self):
        self.gen_users()

        self.start_connection_time = perf_counter()
        await self.connect_clients()
        self.stop_connection_time = perf_counter()

        self.start_insertion_time = perf_counter()
        if self.configuration['INTERVAL'] == 'timeless':
            logging.debug(f'start timeless')
            await self.timeless()
        else:
            logging.debug(f'start interval')
            await self.interval()
        
        print('Closing connections')
        while len(asyncio.all_tasks(asyncio.get_running_loop())) > 1:
            await asyncio.sleep(0)
        await self.close_connections()
        self.metrics()


async def main(test):
    start_test = perf_counter()
    await test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return

async def main_main(test):
    start_test = perf_counter()
    #test = SpectatorTesting(configuration=configuration)
    await test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return 0

if __name__ == '__main__':
    configuration = read_config()
    test = SpectatorTesting(configuration=configuration)
    try:
        asyncio.run(main(test))
    except KeyboardInterrupt:
        print('KB interrupt')
        test.interruption_close_connections()
        test.metrics()


