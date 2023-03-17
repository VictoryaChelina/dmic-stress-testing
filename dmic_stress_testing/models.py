import abc
import infi.clickhouse_orm as ico
from enum import Enum


class MarkOperationType(Enum):
    SCREEN = 1
    PRINT  = 2

def date_field():
    def wrapper_date_field(field):
        str(field)
    return wrapper_date_field()


class TTLTable(ico.Model):
    ttl_amount = 1
    ttl_interval = 'MONTH'

    @classmethod
    def ttl_date(cls) -> ico.DateField:
        '''Return DateField column for table'''
        raise NotImplementedError(f'In class {cls}')

    @classmethod
    def set_ttl(cls, db):
        sql = f'ALTER TABLE {cls.table_name()} MODIFY TTL {cls.ttl_date()} + INTERVAL {cls.ttl_amount} {cls.ttl_interval};'
        db._send(sql)


class screenmarkfact(TTLTable):
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
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.dt


class printmarkfact(screenmarkfact):
    pass
    

class TTLTableFromDT(TTLTable):
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.dt


class pc_activity(TTLTableFromDT):
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


class pc_first_last_seen(TTLTable):
    root_disk_serial = ico.StringField()
    first_seen = ico.DateField()
    last_seen = ico.DateField()
    engine = ico.ReplacingMergeTree(
        partition_key=['toYYYYMM(last_seen)'],
        order_by=[root_disk_serial],
        index_granularity=8192)
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.last_seen


class department_activity(TTLTable):
    dt = ico.DateField()
    department = ico.UInt32Field()
    row_count = ico.Int32Field()
    engine = ico.SummingMergeTree(
        summing_cols = ['row_count'],
        partition_key=['toYYYYMM(dt)'],
        order_by=[department, dt],
        index_granularity=8192)
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.dt


class mark_activity(TTLTable):
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
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.dt


class marker_first_last_seen(TTLTable):
    marker = ico.StringField()
    first_seen = ico.DateField()
    last_seen = ico.DateField()
    engine = ico.ReplacingMergeTree(
        partition_key=['toYYYYMM(last_seen)'],
        order_by=[marker],
        index_granularity=8192)
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.last_seen


class markfact(TTLTable):
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
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.dt


class stats_by_date(TTLTable):
    dt = ico.DateField()
    row_count = ico.Int32Field()
    engine = ico.SummingMergeTree(
        summing_cols = ['row_count'],
        partition_key=['toYYYYMM(dt)'],
        order_by=[dt],
        index_granularity=8192)
    
    @classmethod
    def ttl_date(cls) -> ico.DateField:
        return cls.dt
