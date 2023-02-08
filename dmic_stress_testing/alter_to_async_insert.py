from dmic_stress_testing.checker import process
from dmic_stress_testing.script_parser import read_config
from tqdm import tqdm


def alter_insert(configuration):
    pbar = tqdm(total=65355, desc='Altering insertion mode')
    admin = process()
    for user in range(0, 65355):
        department_num = f'{user:05}'
        max_data = configuration['ASYNC_INSERT']['MAX_DATA_SIZE']
        timeout = configuration['ASYNC_INSERT']['BUSY_TIMEOUT']
        query = f'ALTER USER department{department_num} SETTINGS async_insert = 1, async_insert_max_data_size = {max_data}, async_insert_busy_timeout_ms = {timeout}'
        admin.raw(query)
        pbar.update(1)
    pbar.close()


if __name__ == '__main__':
    configuration = read_config()
    alter_insert(configuration)
