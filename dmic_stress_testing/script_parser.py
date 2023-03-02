import argparse
import json


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        type=str,
        help='add path to your custom config'
    )
    parser.add_argument(
        '--db',
        type=str,
        help='add db url like http://10.11.20.224:8123'
    )
    parser.add_argument(
        '--source_ip',
        type=str,
        help='add source ip like 10.11.20.224'
    )
    parser.add_argument(
        '--db_scheme',
        type=str,
        help='add db scheme origin or changed'
    )
    parser.add_argument(
        '--conn-int',
        type=int,
        help='add connection interval (seconds)'
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
        help='add push interval (seconds)'
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
    parser.add_argument(
        '--mode',
        type=str,
        help='choose async or thread mode'
    )
    parser.add_argument(
        '--interval',
        type=str,
        help='add "interval" for time interval or "loops"'
    )
    parser.add_argument(
        '--amount',
        type=int,
        help='add amount of loops or seconds for test'
    )
    parser.add_argument(
        '--log',
        type=str,
        help='add log file name'
    )
    parser.add_argument(
        '--async_insert',
        type=bool,
        help='set this flag for using async insert to ClickHouse needed'
    )
    parser.add_argument(
        '--insert_max_data_size',
        type=int,
        help='add maximum amount of rows per async insertion \
            (if --async_insert set)'
    )
    parser.add_argument(
        '--insert_busy_timeout',
        type=int,
        help='add timeout for async insertion in ms (if --async_insert set)'
    )
    args = parser.parse_args()
    return args


def read_config():
    conf = parser()
    from dmic_stress_testing.config import configuration
    result_config = configuration

    if conf.config is not None:
        with open(conf.config) as file:
            alter_conf = json.load(file)
            for property, value in alter_conf.items():
                if type(value) == dict:
                    for prop in value.keys():
                        result_config[property][prop] = value[prop]
                else:
                    result_config[property] = alter_conf[property]

    if conf.db is not None:
        result_config["DB_URL"] = conf.db
    if conf.source_ip is not None:
        result_config["SOURCE_IP"] = conf.source_ip
    if conf.db_scheme is not None:
        result_config["DB_SCHEME"] = conf.db_scheme
    if conf.conn_int is not None:
        result_config["CONNECTION_INTERVAL"] = conf.conn_int
    if conf.rows is not None:
        result_config["ROWS_NUM"] = conf.rows
    if conf.users is not None:
        result_config["USERS_NUM"] = conf.users
    if conf.depart is not None:
        result_config["DEPARTMENT_NUM"] = conf.depart
    if conf.batch is not None:
        result_config["BATCH_SIZE"] = conf.batch
    if conf.p_int is not None:
        result_config["PUSH_INT"] = conf.p_int
    if conf.m_int is not None:
        result_config["MARK_INTERVAL"] = conf.m_int
    if conf.m_con_at is not None:
        result_config["MAX_CONNECTION_ATTEMPTS"] = conf.m_con_at
    if conf.async_limit is not None:
        result_config["ASYNC_LIMIT"] = conf.async_limit
    if conf.mode is not None:
        result_config["MODE"] = conf.mode
    if conf.interval is not None:
        result_config["INTERVAL"] = conf.interval
    if conf.amount is not None:
        result_config["AMOUNT"] = conf.amount
    if conf.log is not None:
        result_config["LOG"] = conf.log
    if conf.async_insert is not None:
        result_config["ASYNC_INSERT"]["ON"] = conf.async_insert
    if conf.insert_max_data_size is not None:
        result_config["ASYNC_INSERT"]["MAX_DATA_SIZE"] =\
            conf.insert_max_data_size
    if conf.insert_busy_timeout is not None:
        result_config["ASYNC_INSERT"]["BUSY_TIMEOUT"] =\
            conf.insert_busy_timeout

    return result_config
