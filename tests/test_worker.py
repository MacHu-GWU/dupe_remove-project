# -*- coding: utf-8 -*-

import time
from datetime import datetime
import pytest
from dupe_remove.tests import (
    test_db_ready_flag, engine, table_name, id_col_name, sort_col_name,
    n_event, n_total, n_distinct, n_dupes,
)
from dupe_remove.worker import Worker


class TestWorker(object):
    def test_encode(self):
        assert Worker.encode(1) == "1"
        assert Worker.encode(-1) == "-1"
        assert Worker.encode(3.14) == "3.14"
        assert Worker.encode(-3.14) == "-3.14"
        assert Worker.encode("Hello") == "'Hello'"
        assert Worker.encode("this is a 'word!'") == "'this is a ''word!'''"
        assert Worker.encode('this is a "word!"') == "'this is a \"word!\"'"
        assert Worker.encode(datetime(2000, 1, 1)) == "'2000-01-01 00:00:00'"

    def test_count_duplicates(self):
        if test_db_ready_flag:
            worker = Worker(
                engine=engine, table_name=table_name,
                id_col_name=id_col_name, sort_col_name=sort_col_name,
            )
            print(worker.count_duplicates())

    # def test_remove_duplicate(self):
    #     if test_db_ready_flag:
    #         worker = Worker(
    #             engine=engine, table_name=table_name,
    #             id_col_name=id_col_name, sort_col_name=sort_col_name,
    #         )
    #         assert worker.sort_key_min_max() == (1, 1000)
    #         assert worker.count_duplicates() == (n_total, n_distinct, n_dupes)
    #
    #         st = time.clock()
    #         n_period = 10
    #         gap = n_event / n_period
    #         for ith in range(n_period):
    #             lower, upper = ith * gap, (ith + 1) * gap
    #             worker.remove_duplicate(lower, upper)
    #         elapsed = time.clock() - st
    #
    #         assert worker.count_duplicates() == (n_distinct, n_distinct, 0)
    #
    #         print("elapsed: %.6f seconds" % elapsed)
    #     else:
    #         print("Worker.remove_duplicate are not tested!")


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
