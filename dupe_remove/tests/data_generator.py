# -*- coding: utf-8 -*-

"""

table.events:

- id: string
- time: datetime

"""

import uuid
import random
import rolex
import boto3
import pandas as pd
from six import StringIO
from datetime import datetime

ses = boto3.Session(profile_name="identitysandbox.gov")
s3 = ses.resource("s3")
bucket = s3.Bucket("login.gov-dev-sanhe")


def create_test_data(start, n_month, n_rows_each_month, dupe_perc):
    data = list()
    start_eod = datetime(year=start.year, month=start.month,
                         day=start.day, hour=23, minute=59, second=59)

    for end_time in pd.date_range(start=start_eod, periods=24, freq="1m"):
        start_time = datetime(
            year=end_time.year, month=end_time.month, day=1,
            hour=0, minute=0, second=0,
        )
        df = pd.DataFrame()
        df["id"] = [uuid.uuid4() for i in range(n_rows_each_month)]
        df["time"] = pd.date_range(start_time, end_time, n_rows_each_month)

        buffer = StringIO()
        df.to_csv(buffer, index=False, compression="gzip")
        print(buffer.getvalue())
        # fname =
        break
    # print(list(pd.date_range(start=start_eod, periods=12, freq="1m")))


    # n_distinct = n_rows
    # n_dupes = int(n_rows * dupe_perc)
    # n_total = n_distinct + n_dupes
    #
    # test_data = [
    #     {id_col_name: str(uuid.uuid1()), sort_col_name: i}
    #     for i in range(1, n_distinct + 1)
    # ]
    # test_data.extend(random.sample(test_data, n_dupes))
    # random.shuffle(test_data)
    # return test_data, n_total, n_distinct, n_dupes

create_test_data(
    start=datetime(2018, 1, 1),
    n_month=12,
    n_rows_each_month=30,
    dupe_perc=0.1,
)
