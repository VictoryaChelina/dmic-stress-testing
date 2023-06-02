# dmic-stress-testing

## Name

Dmic-stress-testing

## Description

Simple tool to provide stress test on dmic server.

## Installation

1) Install [dm-marker-generator](https://gitlab.ispras.ru/watermarking/dm-marker-generator)

```
pip install https://gitlab.ispras.ru/watermarking/dm-marker-generator
```

2) Install this package with

```
pip install git+https://gitlab.ispras.ru/watermarking/dmic-stress-testing.git
```

## Usage

Независимо от системы нужно увеличить количество динамических tcp портов.
Это можно сделать при помощи следующих команд в терминале:

На винде:

```
netsh int ipv4 set dynamicport tcp start=1025 num=64510
```

На linux:

```
sudo su 
echo 1024 65535 > /proc/sys/net/ipv4/ip_local_port_range (от пользователя с правами администратора)
```

На linux перед запуском скрипта необходимо увеличить максимальное число файлов, которые может открыть один процесс.

Для этого можно воспользоваться командой (действует для одной сессии).

```
ulimit -n <opend_files_max_limit_per_process>
```

opend_files_max_limit_per_process необходимо указать > числа подключений (USERS_NUM * DEPARTMENT_NUM).  

Основная команда для тестирования - "make-stress"
Самым большим приоритетом конфигурации тестирования обладают опции передаваемые напрямую, далее приоритет отдается параметрам из кастомного конфига (если он передан). Наименьшим приоритетом обладает дефолтный конфиг.
Если в кастомном конфиге указать не все параметры, будут использованы параметры из дефолтного конфига.
Все опции и их краткое описание можно посмотреть командой:

```
make-stress -h
```

Более развернутое описание параметров находится в комментариях дефолтного конфига config.py

Примеры:

1) Запуск с дефолтным конфигом

```
make-stress
```

2) Запуск с кастомным конфигом

```
make-stress --config your_config.json
```

3) Запуск с кастомным конфигом и измененным параметром rows

```
make-stress --config altern_config.json --rows 34
```

4) Запуск с измененным параметром rows

```
make-stress --config altern_config.json --rows 34
```

5) Запуск через https (\dmic-stress-testing\crts\chain.pem - готовый сертификат для 10.11.20.144),
В /etc/hosts нужно добавить `10.11.20.144 dmic.com` и перезапустить сервис `sudo systemctl restart networking`

```

make-stress --crt <путь к цепочке сертификатов> --db https://dmic.com:443 
```

Также есть отдельная команды для проверки числа строк в базе - `check`; и команда запрашивающая от кликхауса число partioins - `partitions`. Последнюю необходимо запустить в отдельном терминале перед началом тестирования, а после окончания тестирования прервать ctrl+c и получить график.

## Project status

Задача перегрузить сервер и сделать это правдоподобно.

Строка на пользователе формируется каждые 10 секунд.
От одного пользователя выгрузка каждые 60 секунд. (По 6 строк, если подключен).
Если нет подключения, то накопленные логи пушатся по максимум 100 штук раз в минуту.
100 строк копятся 1000 секунд.

МАКСИМАЛЬНАЯ ЗАГРУЗКА - НАИХУДШИЙ СЦЕНАРИЙ
В любом случае, максимальная нагрузка от одного пользователя 100 строк в минуту.

Нужно дойти до 1 млн пользователей.
Значит в самом плохом случае 1 млн пользователей, попав в один временной промежуток,
будут заваливать сервер 100 млн строк в минуту.
Для этого у 1 млн пользователей должно накопиться хотя бы по 100 строк у каждого.
100 строк копятся, если нет возможности отправить лог 1000 секунд (почти 17 минут).

МИНИМАЛЬНАЯ ЗАГРУЗКА - НАИЛУЧШИЙ СЦЕНАРИЙ
У 1 млн пользователей постоянно отсылаются логи. Значит по 6 строчек от каждого в минуту.
Получается 6 млн строк в минуту от всех.
А в реальности даже лучше, т.к. нет каких-то общих часов,
по которым спектраторы на всех пользователях одновременно бы отправили свои 6 строк из логов.
