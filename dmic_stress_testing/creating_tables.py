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
    #creating_origin_scheme()
    creating_changed_scheme()
