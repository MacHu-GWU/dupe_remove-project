# -*- coding: utf-8 -*-

from __future__ import division
from .scheduler import Scheduler
from .worker import Worker
from .logger import logger


class Handler(object):
    dupe_perc_threshold = 0.0001

    def __init__(self, scheduler, worker):
        if not isinstance(scheduler, Scheduler):
            raise TypeError
        if not isinstance(worker, Worker):
            raise TypeError

        self.scheduler = scheduler
        self.worker = worker
        self.logger = logger

    def handler(self, event, context):
        lower, upper = self.scheduler.lower_and_upper
        n_total, n_distinct, n_dupes = self.worker.count_duplicates()
        dupe_perc = n_dupes / n_distinct
        self.logger.info("remove duplicate where between (%s, %s)" % (lower, upper))
        self.logger.info("n_total = %s, n_distinct = %s, n_dupes = %s, dupe_perc = %.6f" % (
            n_total, n_distinct, n_dupes, dupe_perc
        ))
        if dupe_perc <= self.dupe_perc_threshold:
            msg = ("dupe percentage less thant {dupe_perc_threshold}, "
                   "don't do anything.").format(
                dupe_perc=dupe_perc,
                dupe_perc_threshold=self.dupe_perc_threshold
            )
            self.logger.info(msg)
            return
        self.logger.info("removing duplicate ...")
        self.worker.remove_duplicate(lower, upper)
        self.logger.info("success!")
