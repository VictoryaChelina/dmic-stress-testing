import infi.clickhouse_orm as ico
from dmic_stress_testing.checker import process


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
        partition_key=['dt'],
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
        partition_key=['root_disk_serial', 'dt'],
        order_by=(root_disk_serial, dt),
        index_granularity=8192)


class pc_first_last_seen(ico.Model):
    root_disk_serial = ico.StringField()
    first_seen = ico.DateField()
    last_seen = ico.DateField()
    engine = ico.MergeTree(
        partition_key=['root_disk_serial'],
        order_by=[root_disk_serial],
        index_granularity=8192)


def creating():
    db = process()
    db.create_table(screenmarkfact)
    db.create_table(printmarkfact)
    db.create_table(pc_activity)
    db.create_table(pc_first_last_seen)


if __name__ == '__main__':
    creating()
