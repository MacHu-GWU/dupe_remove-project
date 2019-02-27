# -*- coding: utf-8 -*-


# class Value(object):
#     pass
#
#
# class Delta(object):
#     pass
#
#
# def find_range(start, delta, nth):
#     lower = start + delta * (nth - 1)
#     upper = start + delta * nth
#     return lower, upper

import attr

@attr.s
class ScheduleOptimizer(object):
    min_value = attr.ib()
    max_value = attr.ib()
    n_rows = attr.ib()

    # def optimize(self):


