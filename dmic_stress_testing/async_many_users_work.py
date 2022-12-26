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
    parser.add_argument(
        '--async_limit',
        type=int, 
        help='add async tasks limit'
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

        dt = datetime.datetime.today()
        dtm = datetime.datetime.today()
        report_time = datetime.datetime.today()
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
            self.rows_const_part[id][2] = datetime.datetime.today()
            client = self.connections[id]
            rows = []
            for i in range(self.configuration['ROWS_NUM']):
                self.rows_const_part[id][0] = datetime.datetime.today()
                self.rows_const_part[id][1] = datetime.datetime.today()
                row = self.rows_const_part[id]
                rows.append(row)
            # print(rows)
            row = rows[0]
            await client.execute("INSERT INTO screenmarkfact (*) VALUES", row)
            # await client.execute("INSERT INTO screenmarkfact (*) VALUES", *rows)
            logging.info(f'client with id {id} insert rows')

    async def insert_rows_many_users(self):
        async with ClientSession() as s:
            # futures = [
            #     self.insert_rows_one_user(id=id) for id in self.connections 
            #     if self.last_push_time[id] - datetime.datetime.today() >= 
            #     datetime.timedelta(seconds=self.configuration['PUSH_INT'])
            #     ]
            while True:
                for id in self.connections:
                    if len(asyncio.all_tasks(asyncio.get_running_loop())) < self.configuration["ASYNC_LIMIT"]:
                        asyncio.create_task(self.insert_rows_one_user(id))
                    await asyncio.sleep(0)
                break
            # await asyncio.gather(*futures)

    async def timeless(self):
        while True:
            await self.insert_rows_many_users()
            break
        # time.sleep()

    # Подключение клиента 
    async def connect_client(self, id):
        # async with ClientSession() as s:
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
        async with ClientSession() as s:
            futures = [self.connect_client(id=id) for id in self.users.keys()]
            await asyncio.gather(*futures)
        logging.info(f'clients done')

    async def close_connections(self):
        for id in self.connections:
            client = self.connections[id]
            await client.close()

    async def entr_point(self):
        self.gen_users()
        await self.connect_clients()
        await self.timeless()
        while len(asyncio.all_tasks(asyncio.get_running_loop())) > 1:
            await asyncio.sleep(0)
        await self.close_connections()



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
    if config.async_limit != None:
        configuration["ASYNC_LIMIT"] = config.async_limit
    return configuration


async def main():
    configuration = read_config()
    start_test = perf_counter()
    test = SpectatorTesting(configuration=configuration)
    await test.entr_point()
    stop_test = perf_counter()
    logging.warning(f'test worked in {stop_test-start_test} seconds')
    return 0


if __name__ == '__main__':
    asyncio.run(main())