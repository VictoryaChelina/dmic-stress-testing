from marker_generator.marker_generator import generate_marker
from random import randint
import argparse


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('n', type=int, help='Input dir for source image')
    args = parser.parse_args()
    return args.n


def rand_department():
    return str(randint(0, 9999))


def rand_ip():
    return f'/{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}'


def rand_hw():
    ch = "0123456789abcdefghijklmnopqrstuvwxyz"
    return f'{ch[randint(0, 35)]}{ch[randint(0, 35)]}:{ch[randint(0, 35)]}{ch[randint(0, 35)]}:{ch[randint(0, 35)]}{ch[randint(0, 35)]}:{ch[randint(0, 35)]}{ch[randint(0, 35)]}:{ch[randint(0, 35)]}{ch[randint(0, 35)]}:{ch[randint(0, 35)]}{ch[randint(0, 35)]}'


def rand_disk():
    ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    return ''.join([ch[randint(0, 35)] for _ in range(15)])


def rand_user():
    ch = "abcdefghijklmnopqrstuvwxyz"
    return ''.join([ch[randint(0, 25)] for _ in range(randint(0, 20))])


def rand_domain():
    ch = "abcdefghijklmnopqrstuvwxyz"
    return ''.join([ch[randint(0, 25)] for _ in range(3)])


def rand_marker(department, root_disk_serial, user, domain):
    return generate_marker(department, root_disk_serial, user, domain)


def perpetual_markers():
    while True:
        print(rand_marker())


def limit_markers(n):
    for i in range(n):
        print(rand_marker(rand_department(), rand_disk(), rand_user(), rand_domain()))


if __name__ == "__main__":
    n = parser()
    limit_markers(n)

