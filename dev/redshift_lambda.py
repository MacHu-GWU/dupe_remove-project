# -*- coding: utf-8 -*-

"""
Test dupe_remove with real lambda.

deploy the function ``sanhehu_elephant_db_t_events``

1. run ``reset()``
2. invoke manually once
3. run ``check()``, n_dupes should be reduced.
"""

import sqlalchemy as sa
from sqlalchemy_mate import EngineCreator
from dupe_remove.tests import (
    table_name, id_col_name, sort_col_name,
    t_events, metadata, create_test_data,
)
from dupe_remove.worker import Worker

engine_creator = EngineCreator.from_s3_json(
    bucket_name="login.gov-dev-sanhe",
    key="db/redshift-analytics-dev.json",
    aws_profile="identitysandbox.gov",
)
engine = engine_creator.create_redshift()


def reset():
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


def check():
    worker = Worker(
        engine=engine, table_name=table_name,
        id_col_name=id_col_name, sort_col_name=sort_col_name,
    )
    print(worker.count_duplicates())


if __name__ == "__main__":
    # reset()
    check()
