configuration = {
    "DB_URL" : "http://10.11.20.98:8123",
    "CONNECTION_INTERVAL" : 1,
    "ROWS_NUM" : 6,
    "USERS_NUM" : 1,
    "DEPARTMENT_NUM": 100,
    "BATCH_SIZE" : 100,
    "PUSH_INT" : 0,
    "MARK_INTERVAL" : 10,
    "MAX_CONNECTION_ATTEMPTS" : 10,
    "ASYNC_LIMIT" : 100000,
    "MODE" : "async",  #  Возможные значение: async, thread
    "INTERVAL" : 1  #  Время, на которое запускается тест (в минутах) или timeless, если нужен вечный цикл
}
