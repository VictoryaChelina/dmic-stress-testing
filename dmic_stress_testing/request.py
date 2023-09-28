import datetime
import time
from time import perf_counter
import dmic_stress_testing.random_marker as rm
from dmic_stress_testing.script_parser import read_config
import infi.clickhouse_orm as ico
from dmic_stress_testing.p_database import p_db
from random import randint
import logging
from logging import FileHandler
import threading
import concurrent.futures
import csv
from tqdm import tqdm
import queue
from dmic_stress_testing.models import screenmarkfact, markfact
from random import shuffle

uname_ = f'admin'
pass_ = f'yuramarkin'
db = p_db(
    'dmic',
    timeout=60,
    db_url="http://10.10.35.130:8123",
    username=uname_,
    password=pass_)
rows = db.select("*", screenmarkfact)
print(rows)