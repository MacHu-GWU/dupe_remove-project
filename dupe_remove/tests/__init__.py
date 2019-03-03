# -*- coding: utf-8 -*-

"""
In the test, we assume:

- id_col: id, string, generated by ``import uuid``
- sort_col: time, datetime
"""

from __future__ import division
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import String, DateTime

table_name = "events"
id_col_name = "id"
sort_col_name = "time"

metadata = MetaData()
t_events = Table(
    table_name, metadata,
    Column(id_col_name, String),
    Column(sort_col_name, DateTime),
)
