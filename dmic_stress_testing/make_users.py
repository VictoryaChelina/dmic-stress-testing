from checker import process, counting
import datetime

admin = process()
# admin.raw('CREATE ROW POLICY safety ON dmic USING 0 TO ALL EXCEPT admin;')
# print(admin.raw('SHOW GRANTS;'))
# admin.raw('CREATE USER write_only GRANTEES NONE;')
print('no grants if empty:', admin.raw('SHOW GRANTS FOR write_only;'))
admin.raw('GRANT INSERT ON dmic.* TO write_only;')
admin.raw('GRANT CREATE DATABASE ON dmic.* TO write_only;')
write_only = process('write_only')
print('no grants if empty:', admin.raw('SHOW GRANTS FOR write_only;'))

try:
    rows = counting(write_only)
    print(rows)
except:
    print("this user have write only grants")

print(counting('admin'))
write_only.raw(f'INSERT INTO dmic.screenmarkfact (*) VALUES ();')

