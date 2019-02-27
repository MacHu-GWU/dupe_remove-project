# -*- coding: utf-8 -*-

import time
from datetime import datetime
import pytest
from dupe_remove.scheduler import *

class TestScheduleOptimizer(object):
    def test(self):
        min_value = datetime(2017, 10, 1)
        max_value = datetime(2019, 7, 1)
        n_rows = 1250000000



if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
