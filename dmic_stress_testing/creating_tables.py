import infi.clickhouse_orm as ico
from dmic_stress_testing.connection import process
from enum import Enum
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


def creating_mv(db):
    query1 = '''
        CREATE MATERIALIZED VIEW dmic.markfact_mv00 TO dmic.markfact
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
        CREATE MATERIALIZED VIEW dmic.markfact_mv01 TO dmic.markfact
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


def creating_origin_scheme():
    db = process()
    models = [
        screenmarkfact,
        printmarkfact,
        pc_activity,
        pc_first_last_seen,
        department_activity,
        mark_activity,
        markfact,
        stats_by_date,
        marker_first_last_seen
    ]
    for model in models:
        db.drop_table(model)
        db.create_table(model)
    creating_mv(db)


def creating_changed_scheme():
    db = process()
    models_old = [
        screenmarkfact,
        printmarkfact,]
    models = [
        pc_activity,
        pc_first_last_seen,
        department_activity,
        mark_activity,
        markfact,
        stats_by_date,
        marker_first_last_seen
    ]
    for model in models_old:
        db.drop_table(model)

    for model in models:
        db.drop_table(model)
        db.create_table(model)

    for i in range(2):
        drop_v = f'DROP VIEW IF EXISTS markfact_mv0{i}'
        db._send(drop_v)


if __name__ == '__main__':
    creating_origin_scheme()
    #creating_changed_scheme()
