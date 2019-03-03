# -*- coding: utf-8 -*-

"""
Test dupe_remove with real lambda.

deploy the function ``sanhehu_elephant_db_t_events``

1. run ``reset()``
2. invoke manually once
3. run ``check()``, n_dupes should be reduced.
"""

from sqlalchemy_mate import EngineCreator
from dupe_remove.tests import (
    table_name, id_col_name, sort_col_name,
    t_events, metadata, create_test_data,
)
from dupe_remove.worker import Worker

engine_creator = EngineCreator.from_s3_json(
    bucket_name="login.gov-dev-sanhe",
    key="db/postgres-elephant-dev.json",
    aws_profile="identitysandbox.gov",
)
engine = engine_creator.create_postgresql_psycopg2()


def reset():
    n_distinct = 100
    dupe_perc = 0.3

    # initialize
    t_events.drop(engine, checkfirst=True)
    metadata.create_all(engine)

    test_data, n_total, n_distinct, n_dupes = create_test_data(
        n_distinct, dupe_perc, id_col_name, sort_col_name)
    engine.execute(t_events.insert(), test_data)


def check():
    worker = Worker(
        engine=engine, table_name=table_name,
        id_col_name=id_col_name, sort_col_name=sort_col_name,
    )
    print(worker.count_duplicates())


if __name__ == "__main__":
    # reset()
    check()
