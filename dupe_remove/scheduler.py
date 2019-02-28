# -*- coding: utf-8 -*-

"""
This module tells worker the lower and upper bound of the sort key to work on.
"""

import attr
from datetime import datetime


@attr.s
class Scheduler(object):
    """
    A duplicate data remover is scheduled as a cron job on AWS Lambda.

    For example, if the time is the sort key. Then we can:

    - cron frequency: invoke lambda every hours
    - start: start from 2018-01-01
    - delta: each invoke cleans 30 days data

    If we want to clean data from 2018-01-01 to 2019-01-01

    - bin_size: we need to invoke lambda 12 times. first invoke 2018-01-01 to 2018-01-31
        second invoke 2018-01-31 to 2018-02-28 ...
    """
    cron_freq_in_seconds = attr.ib()
    start = attr.ib(default=None)
    delta = attr.ib(default=None)
    bin_size = attr.ib(default=None)
    bins_optional = attr.ib(default=None)

    @property
    def bins(self):
        if self.bins_optional is None:
            bins = list()
            for i in range(self.bin_size):
                lower = self.start + i * self.delta
                upper = self.start + (i + 1) * self.delta
                bins.append((lower, upper))
            return bins
        return self.bins_optional

    @property
    def lower_and_upper(self):
        bins = self.bins
        seconds_from_epoch = (datetime.now() - datetime(1970, 1, 1)).total_seconds()
        nth_period = int(seconds_from_epoch // self.cron_freq_in_seconds)
        bin_index = nth_period % len(bins)
        lower, upper = bins[bin_index]
        return lower, upper
