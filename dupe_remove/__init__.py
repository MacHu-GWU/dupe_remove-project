# -*- coding: utf-8 -*-

"""
Solution for clean up duplicate data in database efficiently.
"""


from ._version import __version__

__short_description__ = "Build Lambda Function to remove duplicate data from Redshift in minutes."
__license__ = "MIT"
__author__ = "Sanhe Hu"
__author_email__ = "husanhe@gmail.com"
__github_username__ = "MacHu-GWU"

try:
    from .worker import Worker
    from .scheduler import Scheduler
    from .handler import Handler
except ImportError:
    pass
