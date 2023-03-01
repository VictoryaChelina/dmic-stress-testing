configuration = {
    # Адресс Dmic
    # "DB_URL": "http://10.11.20.224:8123",
    # "DB_URL": "http://10.100.150.242:8123",
    # "DB_URL": "http://10.100.151.224:8123",
    # "DB_URL": "http://10.11.20.209:8123",
    "DB_URL": "http://10.11.20.97:8123",

    # Схема базы данных
    # origin - строки в markfact приходят через mv 
    # changed - строки сразу приходят в markfact
    "DB_SCHEME": "changed",

    # Промежутки попыток подключения к БД (в секундах)
    "CONNECTION_INTERVAL": 1,

    # Количество генерируемых строк от одного пользователя в минуту
    "ROWS_NUM": 6,

    # Количество пользователей в департаменте
    "USERS_NUM": 60000,
    # Если USERS_NUM == 1,
    # в каждом департаменте по одному пользователю, следовательно,
    # у каждого пользователя будет оригинальные логины и пароли
    # для подключения к базе.
    # Всего пользователей в таком случае будет
    # DEPARTMENT_NUM * 1 = DEPARTMENT_NUM

    # Если USERS_NUM > 1, пользователи одного департамента
    # будет подключаться по одному логину и паролю.
    # Следовательно, всего пользователей будет DEPARTMENT_NUM * USERS_NUM

    # Количество департаментов
    "DEPARTMENT_NUM": 1,

    # Количество строк отправляемых за одну загрузку
    # с одного пользователя (в оригинале 100)
    # Используется только в many_users_work
    "BATCH_SIZE": 100,

    # Время между отправкой update от пользователя в базу (в секундах)
    "PUSH_INT": 60,

    # Промежутки между фактами маркирования на пользователе (в секнудах)
    "MARK_INTERVAL": 10,

    # Максимальное число попыток подключения к базе для одного пользователя
    # Используется только в many_users_work
    "MAX_CONNECTION_ATTEMPTS": 10,

    # Максимальное число одновременно
    # выполняющихся асинхронных задач или тредов
    "LIMIT": 100,

    # Возможные значение: async, thread
    "MODE": "thread",

    # Возможные значения: loops или interval
    "INTERVAL": 'loops',

    # Количество циклов или секунд
    "AMOUNT": 10,

    # Имя файла для записи лога
    "LOG": "60_minuts_40000_users.csv",

    # Включает асинхронную вставку в dmic
    "ASYNC_INSERT": {
        "ON": False,
        "MAX_DATA_SIZE": 100000,  # в байтах
        "BUSY_TIMEOUT": 3000
        }
}


if __name__ == '__main__':
    print(configuration)
