# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import pytest
from dupe_remove.scheduler import Scheduler


class TestScheduleOptimizer(object):
    def test(self):
        cron_freq_in_sec = 3600
        start = datetime(2018, 1, 1)
        delta = timedelta(days=30)
        bin_size = 12

        scheduler = Scheduler(cron_freq_in_sec, start, delta, bin_size)
        bins = scheduler.bins
        lower, upper = scheduler.lower_and_upper

        assert len(bins) == bin_size
        assert (lower, upper) in bins


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
