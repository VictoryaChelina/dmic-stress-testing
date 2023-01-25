import logging
from logging import FileHandler
logging.basicConfig(level=logging.WARNING,handlers=[FileHandler('err_log4.txt')])

import datetime
from time import perf_counter
import dmic_stress_testing.random_marker as rm
from dmic_stress_testing.common import read_config
from random import randint
import numpy as np
import csv

import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession
from aiohttp import TCPConnector 


        

# Класс отправки псевдологов
class SpectatorTesting:
    def __init__(self, configuration):
        self.configuration = configuration
        self.f = open('./' + self.configuration['LOG'], 'w', newline='')

    connections = {}  # Словарь id: ico.Database (экземпляры подключения)
    users = {}  # Словарь id: rm.RandUser 
    rows_const_part = {}  # Словарь id: ScreenmarkFact подготовленная строка для пушинга (неизменяемая часть)
    last_push_time = {}  # Словарь id: время последней отправки с пользователя
    user_rows_count = {}  # Словарь id: всего строк отправлено от пользователя
    tasks = []
    session = None

    # Сбор метрик
    loop_counting = 0
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
            department = randint(1, 65355)
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
                    user.department,
                    user.disk,
                    user.marker,
                    user.ip,
                    user.hw]
    
    # Вставка строки
    async def insert_rows_one_user(self, id):
        report_time = datetime.datetime.today()
        mark_time = report_time
        self.rows_const_part[id][2] = report_time
        client : ChClient = self.connections[id]
        delta = datetime.timedelta(seconds=self.configuration['MARK_INTERVAL'])
        rows = []
        for i in range(self.configuration['ROWS_NUM'], 0, -1):
            mark_time = report_time - delta * i
            self.rows_const_part[id][0] = mark_time
            self.rows_const_part[id][1] = mark_time
            row = self.rows_const_part[id]
            rows.append(row)
        await client.execute("INSERT INTO screenmarkfact VALUES", *rows)

        self.last_insertion_time = perf_counter()
        self.total_user_push += self.configuration['ROWS_NUM']
        self.user_rows_count[id] += self.configuration['ROWS_NUM']
        time_from_start = self.last_insertion_time - self.start_insertion_time
        rps = self.total_user_push / time_from_start
        self.rows_per_second.append(rps)
        print(f'rps: {rps}', end='\r')
        writer = csv.writer(self.f, delimiter=',', quotechar='|')
        writer.writerow([time_from_start, self.total_user_push, rps, datetime.datetime.today()])

    async def insert_rows_many_users(self):
        push_int = datetime.timedelta(seconds=self.configuration['PUSH_INT'])
        for id in self.connections:
            async_condition = len(asyncio.all_tasks(asyncio.get_running_loop())) < self.configuration["LIMIT"] 
            push_condition = datetime.datetime.today() - self.last_push_time[id] >= push_int
            if async_condition and push_condition:
                logging.debug(f'{id} insert rows')
                task = asyncio.create_task(self.insert_rows_one_user(id))
                self.tasks.append(task)
                self.last_push_time[id] = datetime.datetime.today()
            else:
                await asyncio.sleep(0)
            # try:
            #     done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_EXCEPTION)
            #     for task in pending:
            #         task.cancel()
            #         logging.debug(f'task was canceled')
            #     return True
            # except asyncio.exceptions.CancelledError:
            #     print("KB interrupt inside insert_rows_many_users")
            #     return False

    async def loops(self):
        amount = range(self.configuration['AMOUNT'])
        futures = [self.insert_rows_many_users() for _ in amount]
        await asyncio.gather(*futures)

    async def interval(self):
        start_interval = datetime.datetime.today()
        amount = int(self.configuration['AMOUNT'])
        delta = datetime.timedelta(seconds=amount)
        self.loop_counting = 0
        while (datetime.datetime.today() - start_interval < delta):
            await self.insert_rows_many_users()
            self.loop_counting += 1

    # Подключение клиента 
    async def connect_client(self, id, session):
        user = self.users[id]
        department_number = int(user.department)
        uname_ = f'department{department_number:05}'
        pass_ = f'pass{department_number:05}'
        client = ChClient(
            session=session,
            url=self.configuration['DB_URL'],
            database='dmic',
            user=uname_,
            password=pass_)
        self.connections[id] = client
        await client.is_alive()
        logging.info(f'client with id {id} connected')
    
    # Цикл по клиентам 
    async def connect_clients(self):
        self.session = ClientSession(connector=TCPConnector(limit=0))
        futures = [self.connect_client(id=id, session=self.session) for id in self.users.keys()]
        await asyncio.gather(*futures)
        logging.info(f'clients done')

    async def close_connections(self):
        futures = [self.connections[id].close() for id in self.connections]
        await asyncio.gather(*futures)
    
    def interruption_close_connections(self):
        cnt = 0
        for id in self.connections:
            client = self.connections[id]
            asyncio.run(client.close())
            cnt += 1
            logging.debug(f'all connections{len(self.connections)}, closed{cnt}')

    def metrics(self):
        average_rps = self.total_user_push / (self.last_insertion_time - self.start_insertion_time)

        padding = 40
        print('МЕТРИКИ:\n')
        
        if self.configuration['INTERVAL'] == 'interval':
            print(':', 'За было пройдено циклов:'.ljust(padding), self.loop_counting)
        print(':', 'LIMIT:'.ljust(padding), self.configuration['LIMIT'])
        print(':', 'Number of departments:'.ljust(padding), self.configuration['DEPARTMENT_NUM'])
        print(':', 'Number of users per department:'.ljust(padding), self.configuration['USERS_NUM'])
        print(':', 'Total number of users:'.ljust(padding), self.configuration['USERS_NUM'] * self.configuration['DEPARTMENT_NUM'])
        print(':', 'Total number of rows:'.ljust(padding), self.total_user_push, '\n')

        print(':', 'Время на подключение:'.ljust(padding), self.stop_connection_time - self.start_connection_time, '\n')

        print(':', 'Средний rps:'.ljust(padding), average_rps, '\n')
        print(':', 'Время окончания теста:'.ljust(padding), datetime.datetime.today(), '\n')

    async def entr_point(self):
        self.gen_users()

        self.start_connection_time = perf_counter()
        await self.connect_clients()
        self.stop_connection_time = perf_counter()

        self.start_insertion_time = perf_counter()
        if self.configuration['INTERVAL'] == 'loops':
            logging.warning(f'start loops')
            await self.loops()
        else:
            logging.warning(f'start interval')
            await self.interval()
        
        while len(asyncio.all_tasks(asyncio.get_running_loop())) > 1:
            await asyncio.sleep(0)

        print('Closing connections')
        task = asyncio.create_task(self.close_connections())
        await asyncio.wait([task])
        self.metrics()


async def main(test):
    start_test = perf_counter()
    await test.entr_point()
    stop_test = perf_counter()
    print(f'test worked in {stop_test-start_test} seconds')
    return


if __name__ == '__main__':
    configuration = read_config()
    test = SpectatorTesting(configuration=configuration)
    # try:
    asyncio.run(main(test))
    # except KeyboardInterrupt:
    #     print('KB interrupt')
    #     test.interruption_close_connections()
    #     test.metrics()


