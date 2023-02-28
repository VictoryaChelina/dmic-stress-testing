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




def creating():
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


if __name__ == '__main__':
    creating()
