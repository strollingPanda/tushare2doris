import string
import random
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
import time

import pandas as pd


def generate_int(min_value, max_value):
    return random.randint(min_value, max_value)


def generate_decimal():
    return Decimal(random.uniform(-100000, 100000)).quantize(Decimal('0.0000000001'))


def generate_float():
    random_float = random.uniform(0, 100)
    return random_float


def generate_timestamp():
    # 获取当前时间的秒级时间戳
    current_time = int(time.time())

    # 生成一个随机毫秒部分，确保13位长度
    random_milliseconds = random.randint(100, 999)

    # 合并当前时间和随机毫秒部分，生成13位时间戳
    timestamp = int(str(current_time) + str(random_milliseconds))
    return timestamp


# 生成随机字符串
def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


# 生成随机日期时间
def generate_random_datetime(start_date, end_date):
    time_diff = end_date - start_date
    random_time_diff = random.random() * time_diff.total_seconds()
    random_timedelta = timedelta(seconds=random_time_diff)
    return start_date + random_timedelta


def generate_boolean():
    return random.choice([True, False])


def get_test_data(data_num: int):
    list = []
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 9, 9)
    for i in range(data_num):
        line = (i,
                generate_int(0, 127),
                generate_int(0, 32767),
                generate_int(1000, 1000000),
                generate_int(0, 1000000000000),
                generate_random_string(10),
                generate_random_string(12),
                generate_random_string(20),
                generate_random_datetime(start_date, end_date))
        list.append(line)
    return list


if __name__ == '__main__':
    for i in range(0, 100):
        print(generate_boolean())
