# -*- coding: utf-8 -*-

import random
import uuid
from six import text_type as str
from sqlalchemy import create_engine, MetaData, Table, Column, Index
from sqlalchemy import String, Integer
from sqlalchemy import select, func

# conn_str = "sqlite:///:memory:"
conn_str = "postgres://cxjclqmd:GhW_zB30KTz3doy3ZOtyzMlxzOiMNBbH@elmer.db.elephantsql.com:5432/cxjclqmd"
table_name = "events"
id_col_name = "id"
sort_col_name = "ts"

metadata = MetaData()
t_events = Table(
    table_name, metadata,
    Column(id_col_name, String),
    Column(sort_col_name, Integer),
)
ind_events = Index("idx_col_id_and_ts", t_events.c.id, t_events.c.ts)


def create_test_data(n_rows, dupe_perc, id_col_name, sort_col_name):
    n_distinct = n_rows
    n_dupes = int(n_rows * dupe_perc)
    n_total = n_distinct + n_dupes

    test_data = [
        {id_col_name: str(uuid.uuid1()), sort_col_name: i}
        for i in range(1, n_distinct + 1)
    ]
    test_data.extend(random.sample(test_data, n_dupes))
    random.shuffle(test_data)
    return test_data, n_total, n_distinct, n_dupes


n_event = 1000
dupe_perc = 0.2
event_data, n_total, n_distinct, n_dupes = create_test_data(
    n_event, dupe_perc, id_col_name, sort_col_name
)

try:
    engine = create_engine(conn_str)


    def drop_all_tables():
        try:
            t_events.drop(engine)
        except:
            pass

        try:
            ind_events.drop(engine)
        except:
            pass


    # drop_all_tables() # reset the database
    metadata.create_all(engine)

    if engine.execute(
            select([func.count(t_events.columns[id_col_name])])
    ).fetchall()[0][0] == 0:
        engine.execute(t_events.insert(), event_data)

    test_db_ready_flag = True
except:
    engine = None
    test_db_ready_flag = False
