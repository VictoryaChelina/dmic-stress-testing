import argparse
from dmic_stress_testing.connection import process
from dmic_stress_testing.p_database import p_db
from dmic_stress_testing.models import \
    screenmarkfact,\
    printmarkfact,\
    pc_activity,\
    pc_first_last_seen,\
    department_activity,\
    mark_activity,\
    markfact,\
    stats_by_date,\
    marker_first_last_seen


#  т.к. библотека не позволяет добавить TTL
from dmic_stress_testing.r_sql_tables import \
    screenmarkfact as r_screenmarkfact,\
    printmarkfact as r_printmarkfact,\
    pc_activity as r_pc_activity,\
    pc_first_last_seen as r_pc_first_last_seen,\
    department_activity as r_department_activity,\
    mark_activity as r_mark_activity,\
    markfact as r_markfact,\
    stats_by_date as r_stats_by_date,\
    marker_first_last_seen as r_marker_first_last_seen


changed_tables = [
    pc_activity,
    pc_first_last_seen,
    department_activity,
    mark_activity,
    markfact,
    stats_by_date,
    marker_first_last_seen
]

original_tables = [
    screenmarkfact,
    printmarkfact,
] + changed_tables


changed_tables_ttl = [
    r_pc_activity,
    r_pc_first_last_seen,
    r_department_activity,
    r_mark_activity,
    r_markfact,
    r_stats_by_date,
    r_marker_first_last_seen
]

original_tables_ttl = [
    r_screenmarkfact,
    r_printmarkfact,
] + changed_tables_ttl

tables_names = [
    'screenmarkfact',
    'printmarkfact',
    'pc_activity',
    'pc_first_last_seen',
    'department_activity',
    'mark_activity',
    'markfact',
    'stats_by_date',
    'marker_first_last_seen'
]

def creating_mv(db):
    query1 = '''
        CREATE MATERIALIZED VIEW IF NOT EXISTS dmic.markfact_mv00 TO dmic.markfact
    (

        `dt` Date,

        `dtm` DateTime,

        `report_time` DateTime,

        `user_name` String,

        `user_domain` String,

        `department` UInt32,

        `root_disk_serial` String,

        `marker` String,

        `ipv4_address` IPv4,

        `hw_address` FixedString(17),

        `operation_type` UInt8
    ) AS
    SELECT
        dt,

        dtm,

        report_time,

        user_name,

        user_domain,

        department,

        root_disk_serial,

        marker,

        ipv4_address,

        hw_address,

        1 AS operation_type
    FROM dmic.screenmarkfact;
    '''
    query2 = '''
        CREATE MATERIALIZED VIEW IF NOT EXISTS dmic.markfact_mv01 TO dmic.markfact
    (

        `dt` Date,

        `dtm` DateTime,

        `report_time` DateTime,

        `user_name` String,

        `user_domain` String,

        `department` UInt32,

        `root_disk_serial` String,

        `marker` String,

        `ipv4_address` IPv4,

        `hw_address` FixedString(17),

        `operation_type` UInt8
    ) AS
    SELECT
        dt,

        dtm,

        report_time,

        user_name,

        user_domain,

        department,

        root_disk_serial,

        marker,

        ipv4_address,

        hw_address,

        2 AS operation_type
    FROM dmic.printmarkfact;
    '''
    db._send(query1)
    db._send(query2)


def drop_table(db):
    for table in tables_names:
        drop = f'DROP TABLE IF EXISTS {table}'
        db._send(drop)


def creating_origin_scheme(db):
    drop_table(db)
    for model in original_tables:
        db.create_table(model)
    creating_mv(db)


def creating_changed_scheme(db):
    drop_table(db)
    for model in changed_tables:
        db.create_table(model)
    for i in range(2):
        drop_v = f'DROP VIEW IF EXISTS markfact_mv0{i}'
        db._send(drop_v)


def creating_origin_scheme_ttl(db):
    drop_table(db)
    for model in original_tables:
        db.create_table(model)
        model.set_ttl(db=db)
    creating_mv(db)


def creating_changed_scheme_ttl(db):
    drop_table(db)
    for model in changed_tables:
        db.create_table(model)
        model.set_ttl(db=db)


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--db',
        type=str,
        help='http://localhost:8123/'
    )
    parser.add_argument(
        '--scheme',
        type=int,
        help='original = 0 / changed = 1'
    )
    parser.add_argument(
        '--ttl',
        type=int,
        help='no ttl = 0 / ttl = 1'
    )
    parser.add_argument(
        '--alter_ttl',
        type=str,
        help='DAY / MONTH / YEAR'
    )
    args = parser.parse_args()
    return args


def read_config():
    conf = parser()
    result_config = {}

    if conf.alter_ttl is not None:
        result_config["ALTER_TTL"] = conf.alter_ttl

    else:
        if conf.scheme is not None:
            result_config["SCHEME"] = conf.scheme
        else:
            print("Add db scheme")
            return False

        if conf.ttl is not None:
            result_config["TTL"] = conf.ttl
        else:
            print("Add ttl")
            return False
    
    if conf.db is not None:
        result_config["DB_URL"] = conf.db
    else:
        print("Add db address")
        return False
    return result_config


def set_ttl(db, model):
    if 'dt' in vars(model):
        date_column = 'dt'
    else:
        date_column = 'last_seen'
    db._send(f'ALTER TABLE {model.table_name()} MODIFY TTL {date_column} + INTERVAL {model.ttl_amount} {model.ttl_interval};')


def main():
    configuration = read_config()
    if not configuration:
        return
    db = process(configuration)
    print(configuration)

    if "ALTER_TTL" in configuration.keys():
        set_ttl(db, interval=configuration["ALTER_TTL"])

    else:
        if configuration["SCHEME"]:
            print("Changed scheme (NO screenmark and printmark)")
            if configuration["TTL"]:
                print("With TTL")
                creating_changed_scheme_ttl(db)
            else:
                print("NO TTL")
                creating_changed_scheme(db)
        else:
            print("Original scheme (screenmark and printmark)")
            if configuration["TTL"]:
                print("With TTL")
                creating_origin_scheme_ttl(db)
            else:
                print("NO TTL")
                creating_origin_scheme(db)

if __name__ == '__main__':
    main()
