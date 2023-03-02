from dmic_stress_testing.connection import process
import datetime

admin = process()
# admin.raw('CREATE ROW POLICY safety ON dmic USING 0 TO ALL EXCEPT admin;')

# print(admin.raw('SHOW GRANTS;'))

# admin.raw('CREATE USER write_only GRANTEES NONE;')
# print('no grants if empty:', admin.raw('SHOW GRANTS FOR write_only;'))

# admin.raw('GRANT INSERT(dt,dtm,report_time,user_name,user_domain,marker,department,root_disk_serial,ipv4_address,hw_address) ON dmic.screenmarkfact TO write_only;')
# admin.raw('GRANT CREATE DATABASE ON dmic.* TO write_only;')
# write_only = process('write_only')
# print('no grants if empty:', admin.raw('SHOW GRANTS FOR write_only;'))

# try:
#     rows = counting(write_only)
#     print(rows)
# except:
#     print("this user have write only grants")

# print(counting(admin))
query = f'INSERT INTO dmic.screenmarkfact (*) VALUES (datetime.datetime(1984, 1, 1, 1, 1, 1, 1), datetime.datetime(1984, 1, 1, 1, 1, 1, 1), datetime.datetime(1984, 1, 1, 1, 1, 1, 1), \'name\', \'domain\', \'01011111010001110110100010110111\', \'10168\'), \'PEJI8F691TG1H3XS4BCM\', \'16.196.186.211\', \'JY9SFPHXNOWS\';'
# try:
#     write_only.raw(query)
# except Exception as ex:
#     print(ex)
# print(counting(admin))



# user1 = process('user1')
# admin.raw('REVOKE SELECT ON dmic.* FROM user1;')
# try:
#     user1.raw(query)
# except Exception as ex:
#     print(ex)

# try:
#     rows = counting(user1)
#     print(rows)
#     for row in user1.select("SELECT (*) FROM dmic.screenmarkfact"):
#         print(row)
# except Exception as ex:
#     print(ex)
#     print("this user have write only grants")


# admin.raw('REVOKE SELECT ON dmic.* FROM write_only_1;')


# admin.raw('CREATE USER write_only_1 GRANTEES NONE;')
# admin.raw('GRANT CREATE DATABASE ON dmic.* TO write_only_1;')
# admin.raw('GRANT INSERT(dt,dtm,report_time,user_name,user_domain,marker,department,root_disk_serial,ipv4_address,hw_address) ON dmic.screenmarkfact TO write_only_1;')
# admin.raw('GRANT SELECT(dt,dtm,report_time,user_name,user_domain,marker,department,root_disk_serial,ipv4_address,hw_address) ON dmic.* TO write_only_1;')
# admin.raw('CREATE SETTINGS PROFILE max_rows_to_read_profile SETTINGS max_rows_to_read = 0, max_bytes_to_read = 0 TO write_only_1')
# write_only_1 = process('write_only_1')
# try:
#     write_only_1.raw(query)
# except Exception as ex:
#     print(ex)

# try:
#     rows = counting(write_only_1)
#     print(rows)
#     for row in write_only_1.select("SELECT (*) FROM dmic.screenmarkfact"):
#         print(row)
# except Exception as ex:
#     print(ex)
#     print("this user have write only grants")


admin.raw('ALTER USER default SETTINGS async_insert = 1;')