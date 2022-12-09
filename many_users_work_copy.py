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
ROWS_NUM = 1  # Количество генерируемых строк от одного пользователя в минуту
USERS_NUM = 2 # Количество пользователей
BATCH_SIZE = 100  # Количество строк отправляемых за одну загрузку (в оригинале 100)
PUSH_INT = 60  # Время между отправкой update от пользователя в базу (в секундах)
MARK_INT = 10  # Промежутки между фактами маркирования на пользователе (в секнудах)
LOG_LEVEL = 15


logging.basicConfig(level=logging.INFO)
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

    # Генерируется заданное число пользователей
    def gen_users(self):
        for user in range(USERS_NUM):
            user = rm.RandUser()
            self.USERS[user.user_id()] = user
            self.CONNECTIONS[user.user_id()] = None
            self.LAST_PUSH_TIME[user.user_id()] = datetime.datetime.today() - datetime.timedelta(minutes=1)
            self.USER_ROWS_COUNT[user.user_id()] = 0
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

    # Запускает цикл по CONNECTIONS для отправки логов
    def pushing_updates(self):
        while True:
            for id in self.CONNECTIONS.keys():
                report_time = datetime.datetime.today()  # Время отправки строк лога с клиента на dmic
                if report_time - self.LAST_PUSH_TIME[id] < datetime.timedelta(seconds=PUSH_INT):
                    continue
                else:
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
                    self.USER_ROWS_COUNT[id] += ROWS_NUM
                    start = perf_counter()
                    self.CONNECTIONS[id].insert(rows, BATCH_SIZE)
                    stop = perf_counter()
                    logging.debug(f'Insert time:{stop-start}')
                    self.LAST_PUSH_TIME[id] = report_time
            break

    def entr_point(self):
        self.gen_users()  # Создаются пользователи, подключения и подготавливаются неизменяемые части строк
        self.connect_users()  # Пользователи подключаются к базе
        self.pushing_updates()  # В бесконечном цикле пушатся строки от пользователей



def main():
    test = SpectatorTesting()
    test.entr_point()
    # dms = SpectatorTesting()
    # dms.process()
    # t_start = time.time()
    # print(dms.push_update())
    # print(time.time() - t_start)
    # one_user_screen_log()
    # one_user_print_log()
    return 0


if __name__ == '__main__':
    main()


# # Генератор пользователей (пока пользователи генерируются поочереди)
# def gen_users():
#     users = []
#     for i in range(USERS_NUM):
#         user = gen_rows_one_user()
#         users += user
#     return users


# # Генератор рандомных строк для таблиц screenmarkfact (один пользователь)
# # report_time нужно изменить далее
# # @profile
# def gen_rows_one_user():
#     rows_one_user = []
#     user_name = rm.rand_user()
#     user_domain = rm.rand_domain()
#     department = rm.rand_department()
#     root_disk_serial = rm.rand_disk()
#     marker = rm.rand_marker(department, root_disk_serial, user_name, user_domain)
#     ipv4_address = rm.rand_ip()
#     hw_address = rm.rand_hw()[:17]
#     for i in range(ROWS_NUM):
#         row = ScreenmarkFact(
#             dt = datetime.date.today(),\
#             dtm = datetime.datetime.today(),\
#             report_time = datetime.datetime.today(),\
#             user_name=user_name,\
#             user_domain=user_domain,\
#             department=department,\
#             root_disk_serial=root_disk_serial,\
#             marker=marker,\
#             ipv4_address=ipv4_address,\
#             hw_address=hw_address)
#             #operation_type = Operation.SCREEN if random() < 0.95 else Operation.PRINT)
#         rows_one_user.append(row)
#         # time.sleep(TIME_FACT)
#     return rows_one_user



# def make_path():
#     path = os.getcwd()
#     path = os.mkdir(path + f'\\log\\{str(rm.rand_folder())}\\')
#     return str(path)


# # Генератор файла screenmark_log
# def one_user_screen_log():
#     path = make_path()
#     with open(path + "screenmark_log", "w") as log:
#         rows = gen_rows_one_user()
#         for row in rows:
#             log.write(
#                 f'{str(row.dtm)[:19]},{str(row.dtm)[20:23]} - event_api.py: INFO - screen-marking;' +
#                 f'{row.user_name};' + 
#                 f'{row.user_domain};' +
#                 f'{row.marker};' +
#                 f'{row.department};' +
#                 f'{row.root_disk_serial};' + 
#                 f'{row.ipv4_address};' +
#                 f'{row.hw_address}' + '\n')


# # Генератор файла printmark_log
# def one_user_print_log():
#     path = make_path()
#     with open(path + "screenmark_log", "w") as log:
#         rows = gen_rows_one_user()
#         for row in rows:
#             log.write(
#                 f'{str(row.dtm)[:19]},{str(row.dtm)[20:23]} - event_api.py: INFO - print-marking;' + #  надо уточнить (посмотреть в printmark_log)
#                 f'{row.user_name};' + 
#                 f'{row.user_domain};' +
#                 f'{row.marker};' +
#                 f'{row.department};' +
#                 f'{row.root_disk_serial};' + 
#                 f'{row.ipv4_address};' +
#                 f'{row.hw_address}' + '\n')