# -*- coding: utf-8 -*-

import uuid
import pandas as pd
from datetime import datetime
from s3iotools.io.dataframe import S3Dataframe


def create_test_data(id_col_name,
                     sort_col_name,
                     start,
                     n_month,
                     n_rows_each_month,
                     dupe_perc,
                     s3_resource,
                     bucket_name):
    """
    Create test data for redshift, write data to gzip compressed csv on S3.

    Recommend Setting:

    - start = datetime(2018, 1, 1)
    - n_month = 12
    - n_rows_each_month = 1000000
    - dupe_perc = 0.1

    This takes several minutes
    """
    start_eod = datetime(year=start.year, month=start.month,
                         day=start.day, hour=23, minute=59, second=59)

    s3df = S3Dataframe(s3_resource=s3_resource, bucket_name=bucket_name)
    for end_time in pd.date_range(start=start_eod, periods=n_month, freq="1m"):
        start_time = datetime(
            year=end_time.year, month=end_time.month, day=1,
            hour=0, minute=0, second=0,
        )
        df = pd.DataFrame()
        start_time_prefix = str(start_time.date()) + "-"
        df[id_col_name] = [start_time_prefix + str(i) for i in range(n_rows_each_month)]
        df[sort_col_name] = pd.date_range(start_time, end_time, n_rows_each_month)
        df = pd.concat([df, df.sample(frac=dupe_perc)], axis=0)
        key = "redshift-data/{}.csv.gz".format(start_time.date())
        s3df.df = df
        print("writing %s ..." % key)
        s3df.to_csv(key=key, gzip_compressed=True, index=False, encoding="utf-8")
