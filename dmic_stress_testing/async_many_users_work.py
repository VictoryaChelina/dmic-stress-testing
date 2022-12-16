import datetime
import time
from time import perf_counter
import dmic_stress_testing.random_marker as rm
from random import randint, random
from enum import Enum
import traceback
import logging
import numpy as np
import argparse
import json 

import asyncio
from aiochclient import ChClient
from aiohttp import ClientSession


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        type=str, 
        default='config.json',
        help='give config for sress-test')
    parser.add_argument(
        '--db',
        type=str, 
        help='add DB_URL'
    )
    parser.add_argument(
        '--conn-int',
        type=int, 
        help='add connection interval'
    )
    parser.add_argument(
        '--rows',
        type=int, 
        help='add rows number per one user'
    )
    parser.add_argument(
        '--users',
        type=int, 
        help='add users number per one department'
    )
    parser.add_argument(
        '--depart',
        type=int, 
        help='add departments number'
    )
    parser.add_argument(
        '--batch',
        type=int, 
        help='add batch size'
    )
    parser.add_argument(
        '--p-int',
        type=int, 
        help='add push interval (in seconds)'
    )
    parser.add_argument(
        '--m-int',
        type=int, 
        help='add mark interval for screenmark'
    )
    parser.add_argument(
        '--m-con-at',
        type=int, 
        help='add max connection attempts for 1 user'
    )
    args = parser.parse_args()
    return args
 

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

        dt = datetime.date(1984, 1, 1)
        dtm = datetime.datetime(1984, 1, 1, 1, 1, 1, 1)
        report_time = datetime.datetime(1984, 1, 1, 1, 1, 1, 1)
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
                self.rows_const_part[id] = (
                    datetime.date(1984, 1, 1),
                    datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    datetime.datetime(1984, 1, 1, 1, 1, 1, 1),
                    user.user_name, 
                    user.user_domain,
                    user.marker,
                    user.department,
                    user.disk,
                    user.ip,
                    user.hw)
    
    # Вставка строки
    async def insert_row(self, client, id):
        row = self.rows_const_part[id]
        await client.execute("INSERT INTO screenmarkfact (*) VALUES", row)

    # Подключение клиента и запуск вставки строк 
    async def connect_client(self, id):
        async with ClientSession() as s:
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
            futures = [self.insert_row(client=client, id=id) for row in range(self.configuration['ROWS_NUM'])]
            await asyncio.gather(*futures)
        logging.info(f'client with id {id} connected')
    
    # Цикл по клиентам 
    async def connect_clients(self):
        async with ClientSession() as s:
            futures = [self.connect_client(id=id) for id in self.users.keys()]
            await asyncio.gather(*futures)
        logging.info(f'clients done')

    # Генерация строк
    def gen_rows(self, id, report_time):
        rows = []
        mark_time = report_time  # Время записи в лог на клиенте факта маркирования
        delta = datetime.timedelta(seconds=self.configuration['MARK_INTERVAL'])  # Интервал между фактами маркирования 
        row = self.rows_const_part[id]  # Подготовленная строка по данному пользователю
        for i in range(self.configuration['ROWS_NUM'], 0, -1):
            start = perf_counter()
            mark_time = report_time - delta * i  # Меняется время маркирования для записи строки
            row.dt = mark_time  # В подготовленную строку добавляются отметки времени
            row.dtm = mark_time
            row.report_time = report_time
            stop = perf_counter()
            self.row_generation_time.append(stop-start)
            rows.append(row)
        return rows

    
    # def metrics(self):
    #     row_generation = np.array(self.row_generation_time)
    #     average_row_generation = np.average(row_generation)
    #     rows_num = row_generation.size
    #     total_row_generation = np.sum(row_generation)

    #     user_connection = np.array(self.user_connection_time)
    #     average_user_connection = np.average(user_connection)
    #     connections_num = user_connection.size

    #     row_insertion = np.array(self.row_insertion_time)
    #     average_row_insertion = np.average(row_insertion)

    #     padding = 40
    #     print('МЕТРИКИ:\n')
    #     print('Threading for push updates:'.ljust(padding), THREAD)
    #     print('ThreadPool for connect users:'.ljust(padding), POOL, '\n')

    #     print('Number of departments:'.ljust(padding), self.configuration['DEPARTMENT_NUM'])
    #     print('Number of users per department:'.ljust(padding), self.configuration['USERS_NUM'])
    #     print('Total number of users:'.ljust(padding), self.configuration['USERS_NUM'] * self.configuration['DEPARTMENT_NUM'], '\n')

    #     print('Среднее время на генерацию строки:'.ljust(padding), average_row_generation)
    #     print('Всего строк было сгенерировано:'.ljust(padding), rows_num)
    #     print('Всего времени потрачено:'.ljust(padding), total_row_generation, '\n')

    #     print(average_user_connection)
    #     print('Среднее время подключения к базе:'.ljust(padding), average_user_connection)
    #     print('Всего подключений:'.ljust(padding), connections_num)
    #     print('Всего времени потрачено:'.ljust(padding), self.total_user_connection, '\n')

    #     print('Среднее время вставки строки в базу:'.ljust(padding), average_row_insertion)
    #     print('Всего времени = среднее * кол-во строк'.ljust(padding), average_row_insertion * rows_num)
    #     print('Всего времени потрачено:'.ljust(padding), self.total_user_push, '\n')

    def entr_point(self):
        #start_gen = perf_counter()
        self.gen_users()  # Создаются пользователи, подключения и подготавливаются неизменяемые части строк
        asyncio.run(self.connect_clients())
        # end_gen = perf_counter()
        # logging.warning(f'Generated users {len(self.users)} in {end_gen-start_gen} seconds')
        # self.connect_users()  # Пользователи подключаются к базе
        # end_connect = perf_counter()
        # logging.warning(f'Connected users {len(self.users)} in {end_connect-end_gen} seconds')
        # self.pushing_updates()  # В бесконечном цикле пушатся строки от пользователей
        # end_push = perf_counter()
        # logging.warning(f'pushed rows in {end_push-end_connect} seconds')
        # self.metrics()


def read_config():
    config = parser()
    with open(config.config) as file:
        configuration = json.load(file)
    if config.db != None:
        configuration["DB_URL"] = config.db
    if config.conn_int != None:
        configuration["CONNECTION_INTERVAL"] = config.conn_int
    if config.rows != None:
        configuration["ROWS_NUM"] = config.rows
    if config.users != None:
        configuration["USERS_NUM"] = config.users
    if config.depart != None:
        configuration["DEPARTMENT_NUM"] = config.depart
    if config.batch != None:
        configuration["BATCH_SIZE"] = config.batch
    if config.p_int != None:
        configuration["PUSH_INT"] = config.p_int
    if config.m_int != None:
        configuration["MARK_INTERVAL"] = config.m_int
    if config.m_con_at != None:
        configuration["MAX_CONNECTION_ATTEMPTS"] = config.m_con_at
    return configuration


def main():
    configuration = read_config()
    start_test = perf_counter()
    test = SpectatorTesting(configuration=configuration)
    test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return 0


if __name__ == '__main__':
    main()