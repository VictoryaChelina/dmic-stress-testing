from dmic_stress_testing.models import screenmarkfact, printmarkfact
from dmic_stress_testing.connection import process
import logging
import infi.clickhouse_orm as ico


logging.basicConfig(level=logging.DEBUG)


def reading(connection):
    for row in printmarkfact.objects_in(connection):
        print(row)


def counting(connection, model=screenmarkfact):
    rows = connection.count(model)
    return rows


def realtime_counting(connection):
    while True:
        print(f'rows in base: {connection.count(screenmarkfact)}', end='\r')


def check():
    connection = process()
    print(connection)
    rows = counting(connection)
    print(rows)


if __name__ == '__main__':
    check()
