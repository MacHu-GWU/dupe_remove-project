# -*- coding: utf-8 -*-

from datetime import datetime
import pytest
from dupe_remove.tests import (
    table_name, id_col_name, sort_col_name, metadata, t_events,
    create_test_data,
)
from dupe_remove.tests.with_postgres import engine
from dupe_remove.worker import Worker


class TestWorker(object):
    def test_everything(self):
        metadata.create_all(engine)

        engine.execute(t_events.delete())

        worker = Worker(
            engine=engine, table_name=table_name,
            id_col_name=id_col_name, sort_col_name=sort_col_name,
        )

        assert worker.count_duplicates(lower=datetime(2018, 1, 1)) == (0, 0, 0)
        assert worker.count_duplicates(upper=datetime(2019, 1, 1)) == (0, 0, 0)
        assert worker.count_duplicates() == (0, 0, 0)

        n_distinct = 1000
        dupe_perc = 0.3
        test_data, n_total, n_distinct, n_dupes = create_test_data(
            n_distinct, dupe_perc, id_col_name, sort_col_name)

        engine.execute(t_events.insert(), test_data)
        assert worker.count_duplicates() == (n_total, n_distinct, n_dupes)

        lower = datetime(2018, 1, 1)
        upper = datetime(2019, 1, 1)

        try:
            worker.remove_duplicate(lower, upper, _raise_error=True)
        except InterruptedError:
            pass
        assert worker.count_duplicates() == (n_total, n_distinct, n_dupes)

        st = datetime.now()
        worker.remove_duplicate(lower, upper)
        elapsed = (datetime.now() - st).total_seconds()

        assert worker.count_duplicates() == (n_distinct, n_distinct, 0)
        print("elapsed: %.6f seconds" % elapsed)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
