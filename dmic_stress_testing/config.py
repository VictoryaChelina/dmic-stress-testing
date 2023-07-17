configuration = {
    # Адрес Dmic
    "DB_URL": "http://localhost:8123",

    # Адрес Dmic
    "CRT": "./crts/chain.pem",

    # Адрес атакующей машины
    "SOURCE_IP": None,

    # Константные строки 
    # 1 - от пользователя будут отправляться одинаковые строчки
    "CONST_ROWS": 0,

    # Схема базы данных
    # origin - строки в markfact приходят через mv 
    # changed - строки сразу приходят в markfact
    "DB_SCHEME": "origin",

    # Промежутки попыток подключения к БД (в секундах)
    "CONNECTION_INTERVAL": 1,

    # Количество генерируемых строк от одного пользователя в минуту
    "ROWS_NUM": 6,
    
    # Количество дней вычитаемых из поля dt при создании строк
    # (Если нужно проверить удаление строк по ttl)
    "D_ROWS": 0,

    # Количество пользователей в департаменте
    "USERS_NUM": 1000,
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
    "DEPARTMENT_NUM": 60,

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

    # Возможные значения: loops или interval
    "INTERVAL": 'loops',

    # Количество циклов или секунд
    "AMOUNT": 1,

    # Имя файла для записи лога
    "LOG": None,

}


if __name__ == '__main__':
    print(configuration)
