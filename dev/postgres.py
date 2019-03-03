# -*- coding: utf-8 -*-

from dateutil.tz import tzutc
from datetime import datetime, timedelta
from sqlalchemy_mate import EngineCreator
from dupe_remove.tests import (
    table_name, id_col_name, sort_col_name,
    t_events, metadata, create_test_data,
)
from dupe_remove.scheduler import Scheduler
from dupe_remove.worker import Worker

UTC = tzutc()

n_distinct = 100
dupe_perc = 0.3

engine_creator = EngineCreator.from_s3_json(
    bucket_name="login.gov-dev-sanhe",
    key="db/postgres-elephant-dev.json",
    aws_profile="identitysandbox.gov",
)
engine = engine_creator.create_postgresql_psycopg2()

# initialize
t_events.drop(engine, checkfirst=True)
metadata.create_all(engine)

test_data, n_total, n_distinct, n_dupes = create_test_data(
    n_distinct, dupe_perc, id_col_name, sort_col_name)
engine.execute(t_events.insert(), test_data)

# test state
worker = Worker(
    engine=engine, table_name=table_name,
    id_col_name=id_col_name, sort_col_name=sort_col_name,
)
min_value, max_value = worker.sort_key_min_max()
assert min_value >= datetime(2018, 1, 1)
assert max_value <= datetime(2019, 1, 1)

# setup worker
cron_freq_in_seconds = 3600
start = datetime(2018, 1, 1)
delta = timedelta(days=31)
bin_size = 12
scheduler = Scheduler(cron_freq_in_seconds, start, delta, bin_size)
bins = scheduler.bins

# run worker
assert worker.count_duplicates() == (n_total, n_distinct, n_dupes)

st = datetime.now()
for lower, upper in bins:
    print(lower, upper)
    worker.remove_duplicate(lower, upper)
elapsed = (datetime.now() - st).total_seconds()

assert worker.count_duplicates() == (n_distinct, n_distinct, 0)
print("elapsed: %.6f seconds" % elapsed)
