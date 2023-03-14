markfact = '''
CREATE TABLE dmic.markfact
(

    `dt` Date DEFAULT '1970-01-01',

    `dtm` DateTime DEFAULT '0000000000',

    `report_time` DateTime DEFAULT '0000000000',

    `user_name` String,

    `user_domain` String,

    `department` UInt32,

    `root_disk_serial` String,

    `marker` String,

    `ipv4_address` IPv4,

    `hw_address` FixedString(17),

    `operation_type` Enum8('SCREEN' = 1,
 'PRINT' = 2) DEFAULT 'SCREEN'
)
ENGINE = MergeTree
TTL dt + INTERVAL 1 DAY
PARTITION BY toYYYYMM(dt)
ORDER BY (dtm,
 department)
SETTINGS index_granularity = 8192;
'''

screenmarkfact = '''
CREATE TABLE dmic.screenmarkfact
(

    `dt` Date DEFAULT '1970-01-01',

    `dtm` DateTime DEFAULT '0000000000',

    `report_time` DateTime DEFAULT '0000000000',

    `user_name` String,

    `user_domain` String,

    `marker` String,

    `department` UInt32,

    `root_disk_serial` String,

    `ipv4_address` IPv4,

    `hw_address` FixedString(17)
)
ENGINE = MergeTree
TTL dt + INTERVAL 1 MONTH
PARTITION BY toYYYYMM(dt)
ORDER BY (dtm,
 department)
SETTINGS index_granularity = 8192;
'''

printmarkfact = '''
CREATE TABLE dmic.printmarkfact
(

    `dt` Date DEFAULT '1970-01-01',

    `dtm` DateTime DEFAULT '0000000000',

    `report_time` DateTime DEFAULT '0000000000',

    `user_name` String,

    `user_domain` String,

    `marker` String,

    `department` UInt32,

    `root_disk_serial` String,

    `ipv4_address` IPv4,

    `hw_address` FixedString(17)
)
ENGINE = MergeTree
TTL dt + INTERVAL 1 DAY
PARTITION BY toYYYYMM(dt)
ORDER BY (dtm,
 department)
SETTINGS index_granularity = 8192;
'''

department_activity = '''
CREATE TABLE dmic.department_activity
(

    `dt` Date DEFAULT '1970-01-01',

    `department` UInt32,

    `row_count` Int32
)
ENGINE = SummingMergeTree(row_count)
TTL dt + INTERVAL 12 MONTH
PARTITION BY toYYYYMM(dt)
ORDER BY (department,
 dt)
SETTINGS index_granularity = 8192;
'''

mark_activity = '''
CREATE TABLE dmic.mark_activity
(

    `marker` String,

    `dt` Date DEFAULT '1970-01-01',

    `root_disk_serial` String,

    `user_name` String,

    `user_domain` String,

    `department` UInt32,

    `ipv4_address` IPv4
)
ENGINE = ReplacingMergeTree
TTL dt + INTERVAL 12 MONTH
PARTITION BY toYYYYMM(dt)
ORDER BY (marker,
 dt)
SETTINGS index_granularity = 8192;
'''

marker_first_last_seen = '''
CREATE TABLE dmic.marker_first_last_seen
(

    `marker` String,

    `first_seen` Date DEFAULT '1970-01-01',

    `last_seen` Date DEFAULT '1970-01-01'
)
ENGINE = ReplacingMergeTree
TTL last_seen + INTERVAL 12 MONTH
PARTITION BY toYYYYMM(last_seen)
ORDER BY marker
SETTINGS index_granularity = 8192;
'''

pc_activity = '''
CREATE TABLE dmic.pc_activity
(

    `root_disk_serial` String,

    `dt` Date DEFAULT '1970-01-01',

    `user_name` String,

    `user_domain` String,

    `department` UInt32,

    `ipv4_address` IPv4
)
ENGINE = ReplacingMergeTree
TTL dt + INTERVAL 12 MONTH
PARTITION BY toYYYYMM(dt)
ORDER BY (root_disk_serial,
 dt)
SETTINGS index_granularity = 8192;
'''

pc_first_last_seen = '''
CREATE TABLE dmic.pc_first_last_seen
(

    `root_disk_serial` String,

    `first_seen` Date DEFAULT '1970-01-01',

    `last_seen` Date DEFAULT '1970-01-01'
)
ENGINE = ReplacingMergeTree
TTL last_seen + INTERVAL 12 MONTH
PARTITION BY toYYYYMM(last_seen)
ORDER BY root_disk_serial
SETTINGS index_granularity = 8192;
'''

stats_by_date = '''
CREATE TABLE dmic.stats_by_date
(

    `dt` Date DEFAULT '1970-01-01',

    `row_count` Int32
)
ENGINE = SummingMergeTree(row_count)
TTL dt + INTERVAL 12 MONTH
PARTITION BY toYYYYMM(dt)
ORDER BY dt
SETTINGS index_granularity = 8192;
'''