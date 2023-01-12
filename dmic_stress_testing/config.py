configuration = {
    # Адресс Dmic
    "DB_URL" : "http://10.11.20.98:8123",

    # Промежутки попыток подключения к БД (в секундах)
    "CONNECTION_INTERVAL" : 1,

    # Количество генерируемых строк от одного пользователя в минуту
    "ROWS_NUM" : 1,

    # Количество пользователей в департаменте
    "USERS_NUM" : 1000,
    # Если USERS_NUM == 1, в каждом департаменте по одному пользователю, следовательно,
    # у каждого пользователя будет оригинальные логины и пароли для подключения к базе.
    # Всего пользователей в таком случае будет DEPARTMENT_NUM * 1 = DEPARTMENT_NUM

    # Если USERS_NUM > 1, пользователи одного департамента будет подключаться по одному логину и паролю.
    # Следовательно, всего пользователей будет DEPARTMENT_NUM * USERS_NUM

    # Количество департаментов
    "DEPARTMENT_NUM": 1,

    # Количество строк отправляемых за одну загрузку с одного пользователя (в оригинале 100)
    # Используется только в many_users_work
    "BATCH_SIZE" : 1000,

    # Время между отправкой update от пользователя в базу (в секундах)
    "PUSH_INT" : 0,

    # Промежутки между фактами маркирования на пользователе (в секнудах)
    "MARK_INTERVAL" : 10,

    # Максимальное число попыток подключения к базе для одного пользователя
    # Используется только в many_users_work
    "MAX_CONNECTION_ATTEMPTS" : 10,

    # Максимальное число одновременно выполняющихся асинхронных задач
    "ASYNC_LIMIT" : 10000,

    # Возможные значение: async, thread
    "MODE" : "async",  

    # Возможные значения: loops или interval
    "INTERVAL" : 'loops', 

    # Количество циклов или секунд
    "AMOUNT" : 1,

    #  Имя файла для записи лога
    "LOG" : "some_6.csv"  
}

if __name__ == '__main__':
    padding = 40
    for key, item in configuration.items():
        print(':', key.ljust(padding), item)
