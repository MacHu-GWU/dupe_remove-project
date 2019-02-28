# -*- coding: utf-8 -*-

"""
dev scripts to test the effect.
"""

import os
import json
import time
import boto3
import sqlalchemy as sa
from datetime import datetime
from dupe_remove.tests.with_redshift import (
    create_engine, create_table, create_test_data,
)
from dupe_remove.worker import Worker

aws_profile = "identitysandbox.gov"
bucket_name = "login.gov-dev-sanhe"
table_name = "events"
id_col_name = "id"
sort_col_name = "time"

ses = boto3.Session(profile_name=aws_profile)
s3 = ses.resource("s3")

# create_test_data(
#     id_col_name=id_col_name,
#     sort_col_name=sort_col_name,
#     start=datetime(2018, 1, 1),
#     n_month=12,
#     n_rows_each_month=1000000,
#     dupe_perc=0.1,
#     s3_resource=s3,
#     bucket_name=bucket_name,
# )

db_secret_file = os.path.join(os.path.expanduser("~"), ".db.json")
"""
Put your redshift credential here: ${HOME}/.db.json
{
    "<db_identifier>": {
        "host": ...,
        "port": ...,
        "database": ...,
        "username": ...,
        "name": ...
    }
}
"""
db_identifier = "redshift-dev"

engine = create_engine(db_secret_file, db_identifier)
t_events = create_table(engine, table_name, id_col_name, sort_col_name)


def read_test_aws_credential():
    p = os.path.join(os.path.expanduser("~", ".aws", "credentials.json"))
    data = json.load(open(p, "r"))
    key = data[aws_profile]["aws_access_key_id"]
    secret = data[aws_profile]["aws_secret_access_key"]
    return key, secret


def load_data():
    key, secret = read_test_aws_credential
    sql = sa.text("""
    COPY events (id, time)
    FROM 's3://login.gov-dev-sanhe/redshift-data/'
    access_key_id '{}'
    secret_access_key '{}'
    csv
    IGNOREHEADER 1
    gzip;
    """.format(key, secret))
    engine.execute(sql)


# load_data()


worker = Worker(
    engine=engine, table_name=table_name,
    id_col_name=id_col_name, sort_col_name=sort_col_name,
)
print(worker.count_duplicates())
print(datetime.now())
st = time.clock()
try:
    worker.remove_duplicate(datetime(2018, 2, 1), datetime(2018, 3, 1), _raise_error=True)
except Exception as e:
    print(e)
print(datetime.now())
elapsed = time.clock() - st
print(elapsed)
print(worker.count_duplicates())
