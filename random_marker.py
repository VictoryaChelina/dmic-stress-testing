from marker_generator.marker_generator import generate_marker
from random import randint, random
import argparse


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('n', type=int)
    args = parser.parse_args()
    return args.n


def rand_department():
    return str(randint(1, 65536))


def rand_ip():
    return f'{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}'

# Среди трех основных производителей самый длинный номер = 12 
def rand_hw():
    ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([ch[randint(0, 35)] for _ in range(12)])
    

def rand_disk():
    ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([ch[randint(0, 35)] for _ in range(20)])


# Поставила до 100 символов
def rand_user():
    ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([ch[randint(0, 25)] for _ in range(randint(1, 100))])


# Поставила до 100 символов
def rand_domain():
    ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([ch[randint(0, 25)] for _ in range(randint(1, 100))])


def rand_marker(department, root_disk_serial, user, domain):
    return generate_marker(department, root_disk_serial, user, domain)


def rand_folder():
    ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([ch[randint(0, 25)] for _ in range(7)])


def perpetual_markers():
    while True:
        print(rand_marker())


def limit_markers(n):
    for i in range(n):
        print(rand_marker(rand_department(), rand_disk(), rand_user(), rand_domain()))


if __name__ == "__main__":
     n = parser()
     limit_markers(n)


