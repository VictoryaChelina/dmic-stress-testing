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

    #  Возможные значение: async, thread
    "MODE" : "async",  

    #  Время, на которое запускается тест (в секундах 's', минутах 'm' или часах 'h') 
    #  или timeless, если нужен вечный цикл
    "INTERVAL" : [10, 's']  
}
