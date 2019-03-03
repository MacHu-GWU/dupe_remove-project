# -*- coding: utf-8 -*-

import boto3
from datetime import datetime
from dupe_remove.tests import id_col_name, sort_col_name
from dupe_remove.tests.with_redshift import create_test_data

aws_profile = "identitysandbox.gov"
ses = boto3.Session(profile_name=aws_profile)
s3 = ses.resource("s3")
bucket_name = "login.gov-dev-sanhe"

start = datetime(2018, 1, 1)
n_month = 12
n_rows_each_month = 1000000
dupe_perc = 0.3

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
