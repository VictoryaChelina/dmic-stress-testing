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
    total_user_connection = 0
    total_user_push = 0
    insertion_time = 0
    last_push_pc = None
    start_time = None
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
        async with ClientSession() as s:
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
            self.last_push_pc = perf_counter()
            self.row_insertion_time.append((stop-start)/len(rows))
            self.total_user_push += self.configuration['ROWS_NUM']
            logging.info(f'client with id {id} insert rows')

    async def insert_rows_many_users(self):
        async with ClientSession() as s:
            # while True:
            for id in self.connections:
                if len(asyncio.all_tasks(asyncio.get_running_loop())) < self.configuration["ASYNC_LIMIT"] and \
                datetime.datetime.today() - self.last_push_time[id] >= datetime.timedelta(seconds=self.configuration['PUSH_INT']):
                    asyncio.create_task(self.insert_rows_one_user(id))
                    self.last_push_time[id] = datetime.datetime.today()
                await asyncio.sleep(0)
                # break

    async def timeless(self):
        start = perf_counter()
        start_interval = datetime.datetime.today()
        while True:
            running_start = perf_counter()
            await self.insert_rows_many_users()
            stop = perf_counter()
            self.insertion_time = stop - start
            rps = self.total_user_push/(self.last_push_pc - self.start_time)
            wait = datetime.datetime.today() - start_interval
            print(f'rps: {rps}; wait: {wait}',end='\r')
        #break
        # time.sleep()

    async def interval(self):
        with open('some.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            start = perf_counter()
            start_interval = datetime.datetime.today()
            delta = datetime.timedelta(minutes=int(self.configuration['INTERVAL']))
            while (datetime.datetime.today() - start_interval < delta):
                running_start = perf_counter()
                await self.insert_rows_many_users()
                stop = perf_counter()
                self.insertion_time = stop - start
                rps = self.total_user_push/(self.last_push_pc - self.start_time)
                wait = datetime.datetime.today() - start_interval
                writer.writerow([rps, wait])
                print(f'rps: {rps}; wait: {wait}',end='\r')

    # Подключение клиента 
    async def connect_client(self, id):
        start = perf_counter()
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
        stop = perf_counter()
        self.user_connection_time.append(stop - start)
        self.total_user_connection += 1
        logging.info(f'client with id {id} connected')
    
    # Цикл по клиентам 
    async def connect_clients(self):
        async with ClientSession() as s:
            futures = [self.connect_client(id=id) for id in self.users.keys()]
            await asyncio.gather(*futures)
        logging.info(f'clients done')

    async def close_connections(self):
        for id in self.connections:
            client = self.connections[id]
            await client.close()

    def metrics(self):
        row_generation = np.array(self.row_generation_time)
        average_row_generation = np.average(row_generation)
        rows_num = row_generation.size
        total_row_generation = np.sum(row_generation)

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

        print('Среднее время на генерацию строки:'.ljust(padding), average_row_generation)
        print('Всего строк было сгенерировано:'.ljust(padding), rows_num)
        print('Всего времени потрачено:'.ljust(padding), total_row_generation, '\n')

        print('Среднее время подключения к базе:'.ljust(padding), average_user_connection)
        print('Всего подключений:'.ljust(padding), connections_num)
        print('Всего времени потрачено:'.ljust(padding), self.total_user_connection, '\n')

        print('Среднее время вставки строки в базу:'.ljust(padding), average_row_insertion)
        print('Всего времени = среднее * кол-во строк'.ljust(padding), average_row_insertion * rows_num)
        print('Всего строк было отправлено:'.ljust(padding), self.total_user_push)
        print('Всего времени потрачено:'.ljust(padding), self.insertion_time, '\n')
        print('Время окончания теста:'.ljust(padding), datetime.datetime.today(), '\n')

    async def entr_point(self):
        self.gen_users()
        await self.connect_clients()
        try:
            if self.configuration['INTERVAL'] == 'timeless':
                await self.timeless()
            else:
                await self.interval()
        except:
            print("Interruption")
            while len(asyncio.all_tasks(asyncio.get_running_loop())) > 1:
                await asyncio.sleep(0)
            await self.close_connections()
            self.metrics()
        else:
            while len(asyncio.all_tasks(asyncio.get_running_loop())) > 1:
                await asyncio.sleep(0)
            await self.close_connections()
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
    return 0

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
    asyncio.run(main())