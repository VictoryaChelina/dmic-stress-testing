from dmic_stress_testing.script_parser import read_config
from dmic_stress_testing.many_users_work import main_main as thread_main


def main():
    configuration = read_config()
    print(configuration)
    thread_main(configuration)


if __name__ == '__main__':
    main()
