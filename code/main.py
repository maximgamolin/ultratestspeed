import asyncio
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from time import perf_counter
from functools import reduce
try:
    import psycopg2
except ImportError:
    import psycopg2cffi as psycopg2
    from psycopg2cffi import compat
    compat.register()

import aiopg
import json

POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = '1234'
POSTGRES_DB = 'test_speed'
POSTGRES_HOST = 'host.docker.internal'

def calc_sum_in_db():
    """Тестовая функция которая все считает в базе"""
    with psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER, password=POSTGRES_PASSWORD, host=POSTGRES_HOST) as conn:
        with conn.cursor() as cur:
            cur.execute('select sum(bill) from accounts where char_length(name) = 5')
            return cur.fetchone()[0]


def calc_sum_in_python():
    """Тестовая функция которая достает все строки из базы и и вычисляет сумму всех bill с помощью python"""
    with psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                          password=POSTGRES_PASSWORD, host=POSTGRES_HOST) as conn:
        with conn.cursor() as cur:
            cur.execute('select bill from accounts where char_length(name) = 5')
            result = cur.fetchmany(100)
            sum_of_bill = reduce(lambda x, y: x + y[0], result, 0)
            while result:
                result = cur.fetchmany(100)
                sum_of_bill += reduce(lambda x, y: x + y[0], result, 0)
    return sum_of_bill



def sync_call():
    """Тест трех синхронных вызовов расчета в базе"""
    start_time = perf_counter()
    calc_sum_in_db()
    calc_sum_in_db()
    calc_sum_in_db()
    end_time = perf_counter()
    print(json.dumps({"type":"sync", "calc_type": "db", "time": end_time - start_time}))
    return end_time - start_time


def sync_call_sum():
    """Тест трех синхронных вызовов расчета в python"""
    start_time = perf_counter()
    calc_sum_in_python()
    calc_sum_in_python()
    calc_sum_in_python()
    end_time = perf_counter()
    print(json.dumps({"type": "sync", "calc_type": "py", "time": end_time - start_time}))
    return end_time - start_time


def thread_pool_call():
    """Тредпул трех синхронных вызовов для расчета в базе"""
    start_time = perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(calc_sum_in_db),
            executor.submit(calc_sum_in_db),
            executor.submit(calc_sum_in_db),
        ]
        for future in as_completed(futures):
            pass
    end_time = perf_counter()
    print(json.dumps({"type": "thread_pool", "calc_type": "db", "time": end_time - start_time}))
    return end_time - start_time


def thread_pool_sum_call():
    """Тредпул трех синхронных вызовов для расчета в python"""
    start_time = perf_counter()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(calc_sum_in_python),
            executor.submit(calc_sum_in_python),
            executor.submit(calc_sum_in_python),
        ]
        for future in as_completed(futures):
            pass
    end_time = perf_counter()
    print(json.dumps({"type": "thread_pool", "calc_type": "py", "time": end_time - start_time}))
    return end_time - start_time


def process_pool_call():
    """Процеспул трех синхронных вызовов для расчета в базе"""
    start_time = perf_counter()
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(calc_sum_in_db),
            executor.submit(calc_sum_in_db),
            executor.submit(calc_sum_in_db),
        ]
        for future in as_completed(futures):
            pass
    end_time = perf_counter()
    print(json.dumps({"type": "process_pool", "calc_type": "db", "time": end_time - start_time}))
    return end_time - start_time


def process_pool_sum_call():
    """Процеспул трех синхронных вызовов для расчета в python"""
    start_time = perf_counter()
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(calc_sum_in_python),
            executor.submit(calc_sum_in_python),
            executor.submit(calc_sum_in_python),
        ]
        for future in as_completed(futures):
            pass
    end_time = perf_counter()
    print(json.dumps({"type": "process_pool", "calc_type": "py", "time": end_time - start_time}))
    return end_time - start_time


async def _run_async_q():
    """Асинхронный запрос для расчета в базе"""
    async with aiopg.create_pool(
            f'dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST}') as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('select sum(bill) from accounts where char_length(name) = 5')
                await cur.fetchone()


async def _run_async_q_sum_in_python():
    """Асинхронный запрос для расчета в python"""
    async with aiopg.create_pool(
            f'dbname={POSTGRES_DB} user={POSTGRES_USER} password={POSTGRES_PASSWORD} host={POSTGRES_HOST}') as pool:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('select bill from accounts where char_length(name) = 5')
                result = await cur.fetchmany(100)
                sum_of_bill = reduce(lambda x, y: x + y[0], result, 0)
                while result:
                    result = await cur.fetchmany(100)
                    sum_of_bill += reduce(lambda x, y: x + y[0], result, 0)
    return result


async def _async_with_aiopg():
    """Вызов трех асинхронных функций для расчета в базе"""
    try:
        await _run_async_q()
        await _run_async_q()
        await _run_async_q()
    except Exception:
        pass

async def _async_with_aiopg_sum_in_python():
    """Вызов трех асинхронных функций для расчета в python"""
    try:
        await _run_async_q_sum_in_python()
        await _run_async_q_sum_in_python()
        await _run_async_q_sum_in_python()
    except Exception:
        pass


def async_aiopg_call():
    """Запуск асинхронного эвент лупа для расчета в базе"""
    start_time = perf_counter()
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(_async_with_aiopg())
    end_time = perf_counter()
    print(json.dumps({"type": "async", "calc_type": "db", "time": end_time - start_time}))
    return end_time - start_time


def async_aiopg_sum_call():
    """Запуск асинхронного эвент лупа для расчета в python"""
    start_time = perf_counter()
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(_async_with_aiopg_sum_in_python())
    end_time = perf_counter()
    print(json.dumps({"type": "async", "calc_type": "py", "time": end_time - start_time}))
    return end_time - start_time

# def run(fn, fixture):
#     """Функция отвечает за запуск теста,
#     Создание контейнера
#     На всякий случай ждем когда поднимется база в докере
#     создаем и заполняем таблицу фикстурой
#     запускаем тестовую функцию
#     останавливаем контейнер
#     пишем результат в файл
#     """
#     container = docker_run_container()
#     sleep(15)
#     create_and_fill_table(fixture)
#     result = None
#     try:
#         result = fn()
#     except Exception as e:
#         print(e)
#     finally:
#         docker_stop_container(container)
#
#     with open('result.txt', 'a') as f:
#         print(f'{fixture};{fn.__name__};{result};\n')
#
# def main():
#     """Основная функция по запуску тестов"""
#     print(datetime.now())
#     for fixture in ['first_fixture.txt',
#                     # 'second_fixture.txt',
#                     # 'thrd_fixture.txt', 'fourth_fixture.txt',
#                     # 'fifth_fixture.txt'
#                     ]:
#         for fn in [
#             sync_call, sync_call, sync_call,
#             sync_call_sum, sync_call_sum, sync_call_sum,
#             thread_pool_call, thread_pool_call, thread_pool_call,
#             thread_pool_sum_call, thread_pool_sum_call, thread_pool_sum_call,
#             process_pool_call, process_pool_call, process_pool_call,
#             process_pool_sum_call, process_pool_sum_call, process_pool_sum_call,
#             async_aiopg_call, async_aiopg_call, async_aiopg_call,
#             async_aiopg_sum_call, async_aiopg_sum_call, async_aiopg_sum_call,
#         ]:
#             run(fn, fixture)
#     print(datetime.now())
