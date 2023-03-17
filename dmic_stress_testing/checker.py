import argparse
from dmic_stress_testing.models import markfact
from dmic_stress_testing.connection import process
import logging
import infi.clickhouse_orm as ico


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--db',
        type=str,
        help='http://localhost:8123/'
    )
    args = parser.parse_args()
    return args


def read_config():
    conf = parser()
    result_config = {}
    result_config["DB_URL"] = conf.db
    return result_config


def counting(connection, model=markfact):
    rows = connection.count(model)
    return rows


def realtime_counting(connection):
    while True:
        print(f'rows in base: {connection.count(markfact)}', end='\r')


def check():
    configuration = read_config()
    connection = process(configuration)
    rows = counting(connection)
    print(rows)


if __name__ == '__main__':
    check()
