# -*- coding: utf-8 -*-

"""
Test dupe_remove with a real lambda.

2 seconds for 1000
"""

from datetime import datetime, timedelta
from sqlalchemy_mate import EngineCreator
from dupe_remove.tests import (
    table_name, id_col_name, sort_col_name, t_events, metadata,
)
from dupe_remove.tests.with_postgres import create_test_data
from dupe_remove import Worker, Scheduler, Handler


def create_engine():
    aws_profile = "identitysandbox.gov"
    bucket_name = "login.gov-dev-sanhe"
    key = "db/postgres-elephant-dev.json"

    engine_creator = EngineCreator.from_s3_json(
        bucket_name=bucket_name,
        key=key,
        aws_profile=aws_profile,
    )
    engine = engine_creator.create_postgresql_psycopg2()
    return engine


def check_status():
    engine = create_engine()
    worker = Worker(
        engine=engine, table_name=table_name,
        id_col_name=id_col_name, sort_col_name=sort_col_name,
    )
    print(worker.count_duplicates())


def step1_create_test_data():
    n_distinct = 1000
    dupe_perc = 0.3
    test_data, n_total, n_distinct, n_dupes = create_test_data(
        n_distinct, dupe_perc, id_col_name, sort_col_name)
    return test_data


def step2_put_test_data_in_postgres():
    engine = create_engine()

    # initialize
    t_events.drop(engine, checkfirst=True)
    metadata.create_all(engine)

    test_data = step1_create_test_data()
    engine.execute(t_events.insert(), test_data)


def step3_invoke():
    # Lambda Cron Setting
    cron_freq_in_seconds = 3600
    start = datetime(2018, 1, 1)
    delta = timedelta(days=31)
    bin_size = 12
    scheduler = Scheduler(cron_freq_in_seconds, start, delta, bin_size)

    engine = create_engine()
    worker = Worker(
        engine=engine, table_name=table_name,
        id_col_name=id_col_name, sort_col_name=sort_col_name,
    )
    real_handler = Handler(scheduler, worker)

    # run handler
    print(worker.count_duplicates())

    st = datetime.now()
    real_handler.handler(None, None)
    elapsed = (datetime.now() - st).total_seconds()
    print("elapsed: %.6f seconds" % elapsed)

    print(worker.count_duplicates())


if __name__ == "__main__":
    # step2_put_test_data_in_postgres()
    # step3_invoke()
    check_status()
