import asyncio
import sys
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from random import randrange, choices
from string import ascii_lowercase
from time import sleep, perf_counter
from functools import reduce
from datetime import datetime
import aiopg
import docker
import json
import psycopg2

if sys.version_info[0] == 3 and sys.version_info[1] >= 8 and sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

CREATE_TABLE_Q = """
CREATE TABLE accounts (
	id serial PRIMARY KEY,
	name VARCHAR ( 50 ) NOT NULL,
	bill INT NOT NULL
);
"""

INSERT_Q_PATTERN = """
INSERT INTO accounts  (name, bill)
VALUES
{}
"""
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = '1234'
POSTGRES_DB = 'test_speed'

CONTAINERS = [
    # 'cpython_alpine',
    'cpython_ubuntu',
    'nogil',
    'pypy'
]


class StatsManager:

    @staticmethod
    def build_cpu_metrics(stats: dict):
        cpu_percent = 0.0
        previous_cpu = stats['precpu_stats']['cpu_usage']['total_usage']
        cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - previous_cpu
        previous_system = stats['precpu_stats']['system_cpu_usage']
        system_delta = stats['cpu_stats']['system_cpu_usage'] - previous_system
        if system_delta > 0.0 and cpu_delta > 0.0:
            cpu_percent = (cpu_delta / system_delta) * len(stats['cpu_stats']['cpu_usage']['percpu_usage']) * 100
            return cpu_percent
        else:
            return cpu_percent

    @staticmethod
    def build_memory_metrics(stats: dict):
        mem_usage = stats['memory_stats']['usage']
        mem_limit = stats['memory_stats']['limit']
        if mem_limit <= 0:
            mem_limit = 1
        mem_percent = (mem_usage / mem_limit) * 100
        return mem_percent

def create_first_fixture():
    """Генератор фикстур, нужно менять название текстового файла и колличество записей
        НА выходе получим
        ('gslvd',5)
        ('vxzst',5)
        ('bjffa',7)
        ('lgjqg',9)
    """
    with open('fifth_fixture.txt', 'w') as f:
        for i in range(10000000):
            payload = f"('{''.join(choices(ascii_lowercase, k=5))}',{randrange(-10, 10)})\n"
            f.write(payload)


def docker_run_db_container():
    """Запуск контейнера с постгресом, докер уже должен быть установлен"""
    # print('creating container')
    client = docker.from_env()
    container = client.containers.run(
        'postgres:9.5.10',
        environment={
            'POSTGRES_USER': POSTGRES_USER,
            'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
            'POSTGRES_DB': POSTGRES_DB
        },
        ports={
            '5432/tcp': '5432'
        },
        detach=True
    )
    # print(f'container {container} created')
    return container

def docker_run_db_container_v2(fixture):
    client = docker.from_env()
    image, __ = client.images.build(
        path='./',
        dockerfile=f'containers/postgres.Dockerfile',
        tag='postgres_local'
    )
    container = client.containers.run(
        'postgres_local:latest',
        environment={
            'POSTGRES_USER': POSTGRES_USER,
            'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
            'POSTGRES_DB': POSTGRES_DB
        },
        ports={
            '5432/tcp': '5432'
        },
        detach=True
    )
    sleep(60)
    container.exec_run(f"psql -h 127.0.0.1 -d test_speed -U postgres -f {fixture}")
    return container


def build_python_container(image_name):
    client = docker.from_env()
    image, __ = client.images.build(
        path='./',
        dockerfile=f'containers/{image_name}.Dockerfile',
        tag=image_name
    )
    # client = docker.APIClient()
    # stream = client.build(
    #     path='./',
    #     dockerfile=f'containers/{image_name}.Dockerfile',
    #     tag=image_name
    # )
    # print([i for i in stream])
    return image


def docker_run_py_container(name, function):
    client = docker.from_env()
    command = f"python -c 'import main; main.{function}()'"
    container = client.containers.run(
        f'{name}:latest',
        command,
        detach=True
    )
    return container


def docker_stop_container(container):
    """Остановка контейнера после работы"""
    # print('stop container')
    container.stop()
    # print('container stoped')


def create_and_fill_table(fixture_name):
    """Создание новой базы в контейнере и ее заполнение данными"""
    # print('connect to db')
    with psycopg2.connect(dbname=POSTGRES_DB, user=POSTGRES_USER,
                          password=POSTGRES_PASSWORD, host='localhost') as conn:
        with conn.cursor() as cur:
            # print('create table')
            cur.execute(CREATE_TABLE_Q)
            with open(f'fixtures/{fixture_name}') as f:
                # print('insert values')
                for line in f.readlines():
                    cur.execute(INSERT_Q_PATTERN.format(line))
            conn.commit()



def main():
    result = {}
    for container_name in CONTAINERS:
        result[container_name] = {}
        print(container_name)
        image = build_python_container(container_name)
        for fixture in [
             'first_fixture.dump',
                        'second_fixture.dump',
                        'thrd_fixture.dump',
                        'fourth_fixture.dump'
                        ]:
            print(fixture)
            result[container_name][fixture] = []
            for n, fn in enumerate([
                'sync_call',
                'sync_call', 'sync_call',
                'sync_call_sum',
                'sync_call_sum', 'sync_call_sum',
                'thread_pool_call',
                'thread_pool_call', 'thread_pool_call',
                'thread_pool_sum_call',
                'thread_pool_sum_call', 'thread_pool_sum_call',
                'process_pool_call',
                'process_pool_call', 'process_pool_call',
                'process_pool_sum_call',
                'process_pool_sum_call', 'process_pool_sum_call',
                'async_aiopg_call',
                'async_aiopg_call', 'async_aiopg_call',
                'async_aiopg_sum_call',
                'async_aiopg_sum_call', 'async_aiopg_sum_call',
            ]):

                r_stats = []

                try:
                    db_container = docker_run_db_container_v2(fixture)
                    sleep(60)
                    # db_container = docker_run_db_container()
                    # sleep(60)
                    # create_and_fill_table(fixture)
                    # print('done')
                    # while True:
                    #     sleep(1)
                    container = docker_run_py_container(container_name, fn)
                    while container.status != 'exited':
                        sleep(1)
                        stats = container.stats(stream=False)
                        try:
                            r_stats.append({'cpu': StatsManager.build_cpu_metrics(stats),
                                            'mem': StatsManager.build_memory_metrics(stats)})
                        except Exception:
                            pass
                        container.reload()
                    try:
                        logs = json.loads(container.logs().decode('utf-8'))
                    except Exception:
                        print(container.logs())

                finally:
                    docker_stop_container(db_container)
                    docker_stop_container(container)
                result[container_name][fixture].append(
                    {'logs': logs, 'stats': r_stats}
                )
                print(result[container_name][fixture])

    with open('result.json', 'w') as f:
        f.write(json.dumps(result))
    from pprint import pprint; pprint(result)
    #
    # docker_stop_db_container(db_container)


if __name__ == '__main__':
    main()
