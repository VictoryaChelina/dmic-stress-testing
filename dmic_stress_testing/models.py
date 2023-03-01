import infi.clickhouse_orm as ico
from enum import Enum


class MarkOperationType(Enum):
    SCREEN = 1
    PRINT  = 2


class screenmarkfact(ico.Model):
    dt = ico.DateField()
    dtm = ico.DateTimeField()
    report_time = ico.DateTimeField()
    user_name = ico.StringField()
    user_domain = ico.StringField()
    marker = ico.StringField()
    department = ico.UInt32Field()
    root_disk_serial = ico.StringField()
    ipv4_address = ico.IPv4Field()
    hw_address = ico.FixedStringField(length=17)
    engine = ico.MergeTree(
        partition_key=['toYYYYMM(dt)'],
        order_by=(dtm, department),
        index_granularity=8192)


class printmarkfact(screenmarkfact):
    pass


class pc_activity(ico.Model):
    root_disk_serial = ico.StringField()
    dt = ico.DateField()
    user_name = ico.StringField()
    user_domain = ico.StringField()
    department = ico.UInt32Field()
    ipv4_address = ico.IPv4Field()
    engine = ico.ReplacingMergeTree(
        partition_key=['toYYYYMM(dt)'],
        order_by=(root_disk_serial, dt),
        index_granularity=8192)


class pc_first_last_seen(ico.Model):
    root_disk_serial = ico.StringField()
    first_seen = ico.DateField()
    last_seen = ico.DateField()
    engine = ico.ReplacingMergeTree(
        partition_key=['toYYYYMM(last_seen)'],
        order_by=[root_disk_serial],
        index_granularity=8192)


class department_activity(ico.Model):
    dt = ico.DateField()
    department = ico.UInt32Field()
    row_count = ico.Int32Field()
    engine = ico.SummingMergeTree(
        summing_cols = ['row_count'],
        partition_key=['toYYYYMM(dt)'],
        order_by=[department, dt],
        index_granularity=8192)


class mark_activity(ico.Model):
    marker = ico.StringField()
    dt = ico.DateField()
    root_disk_serial = ico.StringField()
    user_name = ico.StringField()
    user_domain = ico.StringField()
    department = ico.UInt32Field()
    ipv4_address = ico.IPv4Field()
    engine = ico.ReplacingMergeTree(
        partition_key=['toYYYYMM(dt)'],
        order_by=[marker, dt],
        index_granularity=8192)


class marker_first_last_seen(ico.Model):
    marker = ico.StringField()
    first_seen = ico.DateField()
    last_seen = ico.DateField()
    engine = ico.ReplacingMergeTree(
        partition_key=['toYYYYMM(last_seen)'],
        order_by=[marker],
        index_granularity=8192)


class markfact(ico.Model):
    dt = ico.DateField()
    dtm = ico.DateTimeField()
    report_time = ico.DateTimeField()
    user_name = ico.StringField()
    user_domain = ico.StringField()
    department = ico.UInt32Field()
    root_disk_serial = ico.StringField()
    marker = ico.StringField()
    ipv4_address = ico.IPv4Field()
    hw_address = ico.FixedStringField(length=17)
    operation_type = ico.Enum8Field(MarkOperationType)
    engine = ico.MergeTree(
        partition_key=['toYYYYMM(dt)'],
        order_by=(dtm, department),
        index_granularity=8192)


class stats_by_date(ico.Model):
    dt = ico.DateField()
    row_count = ico.Int32Field()
    engine = ico.SummingMergeTree(
        summing_cols = ['row_count'],
        partition_key=['toYYYYMM(dt)'],
        order_by=[dt],
        index_granularity=8192)