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
    engine = ico.MergeTree(partition_key=['dt'], order_by=(dtm, department), index_granularity=8192)


class printmarkfact(screenmarkfact):
    pass


def creating():
    db = process()
    db.create_table(screenmarkfact)
    db.create_table(printmarkfact)


if __name__ == '__main__':
    creating()