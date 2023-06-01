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
        help='Add database url in http://x.x.x.x:x format.',
    )
    parser.add_argument(
        '--source_ip',
        type=str,
        help='Add special source ip if there is additionl interfaces in x.x.x.x format.'
    )
    parser.add_argument(
        '--db_scheme',
        type=str,
        choices=['origin', 'changed'],
        help='Add db scheme origin or changed (default - origin).'
    )
    parser.add_argument(
        '--conn-int',
        type=int,
        help='Add connection interval (default - 1s).'
    )
    parser.add_argument(
        '--d_rows',
        type=int,
        help='Add if you need to test ttl (default - 0 days).'
    )
    parser.add_argument(
        '--rows',
        type=int,
        help='Add rows number per one user (default - 6).'
    )
    parser.add_argument(
        '--users',
        type=int,
        help='Add users number per one department (default - 1000).'
    )
    parser.add_argument(
        '--depart',
        type=int,
        help='Add departments number (default - 60).'
    )
    parser.add_argument(
        '--p-int',
        type=int,
        help='Add push interval (default - 60s).'
    )
    parser.add_argument(
        '--m-int',
        type=int,
        help='Add mark interval for screenmark (default - 10s).'
    )
    parser.add_argument(
        '--m-con-at',
        type=int,
        help='Add max connection attempts for 1 user (default - 10).'
    )
    parser.add_argument(
        '--interval',
        type=str,
        choices=['interval', 'loops'],
        help='Add test duration (default - loops).'
    )
    parser.add_argument(
        '--amount',
        type=int,
        help='Add amount of loops or seconds for test (default - 1).'
    )
    parser.add_argument(
        '--log',
        type=str,
        help='Add log file path (default - stress_test_log.csv).'
    )
    parser.add_argument(
        '--limit',
        type=int,
        help='Add thread limit (default - 100).'
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
            pattern = "https:\/\/\d{1,3}.\d{1,3}.\d{1,3}.\d{1,3}:\d{1,5}"
            # if re.match(string=alter_conf["DB_URL"], pattern=pattern) is None:
            #     sys.exit("Database URL should be in http://x.x.x.x:x format")
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
    if conf.p_int is not None:
        result_config["PUSH_INT"] = conf.p_int
    if conf.m_int is not None:
        result_config["MARK_INTERVAL"] = conf.m_int
    if conf.m_con_at is not None:
        result_config["MAX_CONNECTION_ATTEMPTS"] = conf.m_con_at
    if conf.interval is not None:
        result_config["INTERVAL"] = conf.interval
    if conf.amount is not None:
        result_config["AMOUNT"] = conf.amount
    if conf.log is not None:
        result_config["LOG"] = conf.log
    if conf.limit is not None:
        result_config["LIMIT"] = conf.limit

    return result_config