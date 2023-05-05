import argparse
import json
import sys
import re


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        type=str,
        help='Add path to your custom config in json format.'
    )
    parser.add_argument(
        '--db',
        type=str,
        help='Add database url like http://10.11.20.224:8123.',
    )
    parser.add_argument(
        '--source_ip',
        type=str,
        help='Add source ip like 10.11.20.224.'
    )
    parser.add_argument(
        '--db_scheme',
        type=str,
        choices=['origin', 'changed'],
        help='Add db scheme origin or changed.'
    )
    parser.add_argument(
        '--conn-int',
        type=int,
        help='Add connection interval.'
    )
    parser.add_argument(
        '--d_rows',
        type=int,
        help='Add if you need to test ttl (days).'
    )
    parser.add_argument(
        '--rows',
        type=int,
        help='Add rows number per one user.'
    )
    parser.add_argument(
        '--users',
        type=int,
        help='Add users number per one department.'
    )
    parser.add_argument(
        '--depart',
        type=int,
        help='Add departments number.'
    )
    parser.add_argument(
        '--batch',
        type=int,
        help='Add batch size.'
    )
    parser.add_argument(
        '--p-int',
        type=int,
        help='Add push interval (seconds).'
    )
    parser.add_argument(
        '--m-int',
        type=int,
        help='Add mark interval for screenmark (seconds).'
    )
    parser.add_argument(
        '--m-con-at',
        type=int,
        help='Add max connection attempts for 1 user.'
    )
    parser.add_argument(
        '--async_limit',
        type=int,
        help='Add async tasks limit.'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['async', 'thread'],
        help='Choose test mode.'
    )
    parser.add_argument(
        '--interval',
        type=str,
        choices=['interval', 'loops'],
        help='Add test duration.'
    )
    parser.add_argument(
        '--amount',
        type=int,
        help='Add amount of loops or seconds for test.'
    )
    parser.add_argument(
        '--log',
        type=str,
        help='Add log file path.'
    )
    parser.add_argument(
        '--async_insert',
        type=bool,
        help='Set this flag for using async insert to ClickHouse needed.'
    )
    parser.add_argument(
        '--insert_max_data_size',
        type=int,
        help='Add maximum amount of rows per async insertion \
            (if --async_insert set).'
    )
    parser.add_argument(
        '--insert_busy_timeout',
        type=int,
        help='Add timeout for async insertion in ms (if --async_insert set).'
    )
    args = parser.parse_args()
    return args


def read_config():
    conf = parser()
    from dmic_stress_testing.config import configuration
    result_config = configuration

    msg = "Database url is required for this test"
    if (conf.config is None and conf.db is None):
        sys.exit(msg)

    if conf.config is not None:
        with open(conf.config) as file:
            alter_conf = json.load(file)
            if "DB_URL" not in alter_conf:
                sys.exit(msg)
            pattern = "http:\/\/\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{1,5}"
            if re.match(string=alter_conf["DB_URL"], pattern=pattern) is None:
                sys.exit("Database URL should be in http://x.x.x.x:x format")
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
    if conf.d_rows is not None:
        result_config["D_ROWS"] = conf.d_rows
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

print(read_config())