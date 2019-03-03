# -*- coding: utf-8 -*-

"""
4 sec
"""

import boto3
import sqlalchemy as sa
from datetime import datetime, timedelta
from sqlalchemy_mate import EngineCreator
from dupe_remove import Worker, Scheduler, Handler
from dupe_remove.tests import table_name, id_col_name, sort_col_name, metadata, t_events
from dupe_remove.tests.with_redshift import create_test_data


def step1_create_test_data():
    start = datetime(2018, 1, 1)
    n_month = 12
    n_rows_each_month = 1000000
    dupe_perc = 0.3

    aws_profile = "identitysandbox.gov"
    bucket_name = "login.gov-dev-sanhe"

    ses = boto3.Session(profile_name=aws_profile)
    s3 = ses.resource("s3")

    create_test_data(
        id_col_name=id_col_name,
        sort_col_name=sort_col_name,
        start=start,
        n_month=n_month,
        n_rows_each_month=n_rows_each_month,
        dupe_perc=dupe_perc,
        s3_resource=s3,
        bucket_name=bucket_name,
    )


def create_engine():
    aws_profile = "identitysandbox.gov"
    bucket_name = "login.gov-dev-sanhe"
    key = "db/redshift-analytics-dev.json"

    engine_creator = EngineCreator.from_s3_json(
        bucket_name=bucket_name,
        key=key,
        aws_profile=aws_profile,
    )
    engine = engine_creator.create_redshift()
    return engine


def check_status():
    engine = create_engine()
    worker = Worker(
        engine=engine, table_name=table_name,
        id_col_name=id_col_name, sort_col_name=sort_col_name,
    )
    print(worker.count_duplicates())


def step2_put_test_data_in_redshift():
    engine = create_engine()

    # initialize
    metadata.create_all(engine)
    engine.execute(t_events.delete())

    stmt = sa.text(
        """
        COPY {table_name} ({id_col_name}, {sort_col_name})
        FROM 's3://login.gov-dev-sanhe/redshift-data/'
        iam_role 'arn:aws:iam::894947205914:role/s3-full-access-for-redshift'
        csv
        IGNOREHEADER 1
        gzip;
        """.format(
            table_name=table_name, id_col_name=id_col_name, sort_col_name=sort_col_name
        )).execution_options(autocommit=True)
    engine.execute(stmt)


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
    # step1_create_test_data()
    # step2_put_test_data_in_redshift()
    # step3_invoke()
    check_status()
