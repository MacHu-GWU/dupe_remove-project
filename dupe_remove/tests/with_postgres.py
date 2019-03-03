# -*- coding: utf-8 -*-

"""
docker run --rm --name dupe-remove-test-db -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres
docker container stop dupe-remove-test-db
"""

import random
import uuid
from datetime import datetime, timedelta
from six import text_type as str


def create_test_data(n_distinct, dupe_perc, id_col_name, sort_col_name):
    """
    Create test data for postgres.

    Recommend Setting:

    - n_distinct = 1000
    - dupe_perc = 0.3
    """
    n_dupes = int(n_distinct * dupe_perc)
    n_total = n_distinct + n_dupes

    start_time = datetime(2018, 1, 1)
    seconds_in_year = 365 * 24 * 3600
    delta = timedelta(seconds=seconds_in_year / n_distinct)

    test_data = [
        {
            id_col_name: str(uuid.uuid1()),
            sort_col_name: start_time + (i - 1) * delta
        }
        for i in range(1, n_distinct + 1)
    ]
    test_data.extend(random.sample(test_data, n_dupes))
    random.shuffle(test_data)
    return test_data, n_total, n_distinct, n_dupes


if __name__ == "__main__":
    from dupe_remove.tests import id_col_name, sort_col_name

    test_data, n_total, n_distinct, n_dupes = create_test_data(
        100, 0.2, id_col_name, sort_col_name)
    print(test_data)
