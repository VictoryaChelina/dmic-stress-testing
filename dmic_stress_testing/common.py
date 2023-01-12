import argparse
import sys 
import json 
import asyncio

def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--config',
        type=str,
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
    parser.add_argument(
        '--mode',
        type=str, 
        help='choose async or thread mode'
    )
    parser.add_argument(
        '--interval',
        type=str, 
        help='add time for test or "timeless" if you want timeless test'
    )
    parser.add_argument(
        '--log',
        type=str, 
        help='add log file name'
    )
    args = parser.parse_args()
    return args


def read_config():
    conf = parser()
    sys.path.insert(0, conf.config)
    from dmic_stress_testing.config import configuration
    result_config = configuration
    if conf.config != None:
        with open(conf.config) as file:
            alter_conf = json.load(file)
            for property in alter_conf.keys():
                result_config[property] = alter_conf[property]
    if conf.db != None:
        result_config["DB_URL"] = conf.db
    if conf.conn_int != None:
        result_config["CONNECTION_INTERVAL"] = conf.conn_int
    if conf.rows != None:
        result_config["ROWS_NUM"] = conf.rows
    if conf.users != None:
        result_config["USERS_NUM"] = conf.users
    if conf.depart != None:
        result_config["DEPARTMENT_NUM"] = conf.depart
    if conf.batch != None:
        result_config["BATCH_SIZE"] = conf.batch
    if conf.p_int != None:
        result_config["PUSH_INT"] = conf.p_int
    if conf.m_int != None:
        result_config["MARK_INTERVAL"] = conf.m_int
    if conf.m_con_at != None:
        result_config["MAX_CONNECTION_ATTEMPTS"] = conf.m_con_at
    if conf.async_limit != None:
        result_config["ASYNC_LIMIT"] = conf.async_limit
    if conf.mode != None:
        result_config["MODE"] = conf.mode
    if conf.interval != None:
        result_config["INTERVAL"] = conf.interval
    if conf.log != None:
        result_config["LOG"] = conf.log
    return result_config

if __name__ == '__main__':
    configuration = read_config()
    if configuration["MODE"] == "async":
        from dmic_stress_testing.async_many_users_work import main, SpectatorTesting
        test = SpectatorTesting(configuration=configuration)
        try:
            asyncio.run(main(test))
        except KeyboardInterrupt:
            print('KB interrupt')
            test.interruption_close_connections()
            test.metrics()    
    else:
        from dmic_stress_testing.many_users_work import main_main
        main_main(configuration)