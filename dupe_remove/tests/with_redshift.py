# -*- coding: utf-8 -*-

"""
table.events:

- id: string, random uuid
- time: datetime, from 2018-01-01 to 2018-12-31 23:59:59

10,000,000 distinct rows, 10% duplicate.

This takes several minutes
"""

import json
import uuid
import pandas as pd
import sqlalchemy as sa
from datetime import datetime
from s3iotools.io.dataframe import S3Dataframe




def create_engine(db_secret_file, db_identifier):
    db_secret = json.load(open(db_secret_file, "r"))[db_identifier]
    conn_str = "redshift+psycopg2://{username}:{password}@{host}:{port}/{database}".format(**db_secret)
    engine = sa.create_engine(conn_str)
    return engine


def create_table(engine, table_name, id_col_name, sort_col_name):
    metadata = sa.MetaData()
    t_events = sa.Table(
        table_name, metadata,
        sa.Column(id_col_name, sa.String),
        sa.Column(sort_col_name, sa.DateTime),
    )
    metadata.create_all(engine)
    return t_events


def create_test_data(id_col_name, sort_col_name,
                     start, n_month, n_rows_each_month, dupe_perc,
                     s3_resource, bucket_name):
    start_eod = datetime(year=start.year, month=start.month,
                         day=start.day, hour=23, minute=59, second=59)

    s3df = S3Dataframe(s3_resource=s3_resource, bucket_name=bucket_name)
    for end_time in pd.date_range(start=start_eod, periods=n_month, freq="1m"):
        start_time = datetime(
            year=end_time.year, month=end_time.month, day=1,
            hour=0, minute=0, second=0,
        )
        df = pd.DataFrame()
        df[id_col_name] = [uuid.uuid4() for _ in range(n_rows_each_month)]
        df[sort_col_name] = pd.date_range(start_time, end_time, n_rows_each_month)
        df = pd.concat([df, df.sample(frac=dupe_perc)], axis=0)

        key = "redshift-data/{}.csv.gz".format(start_time.date())
        s3df.df = df
        print("writing %s ..." % key)
        s3df.to_csv(key=key, gzip_compressed=True, index=False, encoding="utf-8")
