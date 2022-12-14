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

    def __init__(self):
        ch = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.department = str(randint(1, 65535))
        self.ip = f'{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}'
        self.hw = ''.join(choices(ch,k=12))
        self.disk = ''.join(choices(ch,k=20))
        self.user_name = ''.join(choices(ch,k=randint(1, 100)))
        self.user_domain = ''.join(choices(ch,k=randint(1, 100)))
        self.marker = generate_marker(str(randint(1, 65535)), self.disk, self.user_name, self.user_domain)
    

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


# def rand_department():
#     return str(randint(1, 65536))


# def rand_ip():
#     return f'{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}.{randint(0, 255)}'

# # Среди трех основных производителей самый длинный номер = 12 
# def rand_hw():
#     ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
#     return ''.join([ch[randint(0, 35)] for _ in range(12)])
    

# def rand_disk():
#     ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
#     return ''.join([ch[randint(0, 35)] for _ in range(20)])


# # Поставила до 100 символов
# def rand_user():
#     ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
#     return ''.join([ch[randint(0, 25)] for _ in range(randint(1, 100))])


# # Поставила до 100 символов
# def rand_domain():
#     ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
#     return ''.join([ch[randint(0, 25)] for _ in range(randint(1, 100))])


# def rand_marker(department, root_disk_serial, user, domain):
#     return generate_marker(department, root_disk_serial, user, domain)


# def rand_folder():
#     ch = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
#     return ''.join([ch[randint(0, 25)] for _ in range(7)])


# def perpetual_markers():
#     while True:
#         print(rand_marker())


# def limit_markers(n):
#     for i in range(n):
#         print(rand_marker(rand_department(), rand_disk(), rand_user(), rand_domain()))


# if __name__ == "__main__":
#      n = parser()
#      limit_markers(n)
#      ex = rand_user