import asyncio
from dmic_stress_testing.script_parser import read_config
from dmic_stress_testing.async_many_users_work import main as async_main, SpectatorTesting
from dmic_stress_testing.many_users_work import main_main as thread_main


def main():
    configuration = read_config()
    print(configuration)
    if configuration["MODE"] == "async":
        test = SpectatorTesting(configuration=configuration)
        try:
            asyncio.run(async_main(test))
        except KeyboardInterrupt:
            print('KB interrupt')
            test.interruption_close_connections()
            test.metrics()    
    else:
        thread_main(configuration)


if __name__ == '__main__':
    main()