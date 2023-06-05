from marker_generator.marker_generator import generate_marker
from random import randint, seed, choices
import argparse


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('n', type=int)
    args = parser.parse_args()
    return args.n


# Класс предназначен для генерации раномного пользователя
class RandUser:
    seed(10)

    def __init__(self, department):
        ch = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.department = str(department)
        self.ip = f'{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}'
        self.hw = ''.join(choices(ch, k=12))
        self.disk = ''.join(choices(ch, k=20))
        self.user_name = ''.join(choices(ch, k=randint(1, 64)))
        self.user_domain = ''.join(choices(ch, k=randint(1, 64)))
        self.marker = generate_marker(
            str(randint(1, 65355)),
            self.disk, self.user_name,
            self.user_domain)

    # Возвращает уникальный id пользователя
    def user_id(self):
        return id(self)

    def user_info(self):
        padding = 13
        print('USER INFORMATION:')
        print('department:'.ljust(padding), self.department)
        print('ip:'.ljust(padding), self.ip)
        print('hw:'.ljust(padding), self.hw)
        print('disk:'.ljust(padding), self.disk)
        print('user_domain:'.ljust(padding), self.user_domain)
        print('marker:'.ljust(padding), self.marker)


if __name__ == "__main__":
    example = RandUser()
    example.user_info()
