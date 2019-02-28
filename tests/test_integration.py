# -*- coding: utf-8 -*-

import pytest
from dupe_remove.tests import (
    test_db_ready_flag, engine, table_name, id_col_name, sort_col_name,
    n_event, n_total, n_distinct, n_dupes,
)
from dupe_remove.worker import Worker


class TestWorkerInTransaction(object):
    def test_remove_duplicate(self):
        if test_db_ready_flag:
            worker = Worker(
                engine=engine, table_name=table_name,
                id_col_name=id_col_name, sort_col_name=sort_col_name,
            )
            assert worker.count_duplicates() == (n_total, n_distinct, n_dupes)
            try:
                worker.remove_duplicate(0, n_event, _raise_error=True)
            except Exception as e:
                print(e)
            assert worker.count_duplicates() == (n_total, n_distinct, n_dupes)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
