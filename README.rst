
.. image:: https://readthedocs.org/projects/dupe_remove/badge/?version=latest
    :target: https://dupe_remove.readthedocs.io/index.html
    :alt: Documentation Status

.. image:: https://travis-ci.org/MacHu-GWU/dupe_remove-project.svg?branch=master
    :target: https://travis-ci.org/MacHu-GWU/dupe_remove-project?branch=master

.. image:: https://codecov.io/gh/MacHu-GWU/dupe_remove-project/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/MacHu-GWU/dupe_remove-project

.. image:: https://img.shields.io/pypi/v/dupe_remove.svg
    :target: https://pypi.python.org/pypi/dupe_remove

.. image:: https://img.shields.io/pypi/l/dupe_remove.svg
    :target: https://pypi.python.org/pypi/dupe_remove

.. image:: https://img.shields.io/pypi/pyversions/dupe_remove.svg
    :target: https://pypi.python.org/pypi/dupe_remove

.. image:: https://img.shields.io/badge/STAR_Me_on_GitHub!--None.svg?style=social
    :target: https://github.com/MacHu-GWU/dupe_remove-project

------


.. image:: https://img.shields.io/badge/Link-Document-blue.svg
      :target: https://dupe_remove.readthedocs.io/index.html

.. image:: https://img.shields.io/badge/Link-API-blue.svg
      :target: https://dupe_remove.readthedocs.io/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Source_Code-blue.svg
      :target: https://dupe_remove.readthedocs.io/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
      :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
      :target: https://github.com/MacHu-GWU/dupe_remove-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
      :target: https://github.com/MacHu-GWU/dupe_remove-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
      :target: https://github.com/MacHu-GWU/dupe_remove-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
      :target: https://pypi.org/pypi/dupe_remove#files


Welcome to ``dupe_remove`` Documentation
==============================================================================

**How come duplicate data in database?**

In OLAP database Redshift, the ``primary_key`` column doesn't apply any restriction due to performance issue. What if our ETL pipeline load duplicate same data multiple times in retry?

**How** dupe_remove **solve the problem?**

``dupe_remove`` use a optimized strategy to remove duplicate precisely and fast. You only need to specify:

- database connection
- table name, id column, sort key column

``dupe_remove`` will do these on your own will:

- remove duplicate data in specified sort key range
- deploy as cron job on AWS Lambda to automatically remove all duplicate data in a table.


Usage Example
------------------------------------------------------------------------------
Our database::

    table.events
    |-- column(id, type=string)         # id column
    |-- column(time, type=timestamp)    # sort key column
    |-- other columns ...


On Local Machine
------------------------------------------------------------------------------

.. code-block:: python

    from datetime import datetime, timedelta
    from sqlalchemy_mate import EngineCreator
    from dupe_remove import Worker

    table_name = "events"
    id_col_name = "id"
    sort_col_name = "time"
    credential_file = "/Users/admin/db.json"
    engine_creator = EngineCreator.from_json(credential_file)
    engine = engine_creator.create_redshift()

    worker = Worker(
        engine=engine,
        table_name=table_name,
        id_col_name=id_col_name,
        sort_col_name=sort_col_name,
    )

    worker.remove_duplicate(
        lower=datetime(2018, 1, 1),
        upper=datetime(2018, 2, 1),
    )


On AWS Lambda Cron Job
------------------------------------------------------------------------------

.. code-block:: python

    def handler(event, context):
        from datetime import datetime, timedelta
        from sqlalchemy_mate import EngineCreator
        from dupe_remove import Scheduler, Worker, Handler

        table_name = "events"
        id_col_name = "id"
        sort_col_name = "time"

        engine_creator = EngineCreator.from_env(prefix="DEV_DB", kms_decrypt=True)
        engine = engine_creator.create_redshift()
        test_connection(engine, 6)

        worker = Worker(
            engine=engine,
            table_name=table_name,
            id_col_name=id_col_name,
            sort_col_name=sort_col_name,
        )

        # run every 5 min, clean 31 days data at a time from 2018-01-01,
        # start over in 12 cycle
        cron_freq_in_seconds = 300
        start = datetime(2018, 1, 1)
        delta = timedelta(days=31)
        bin_size = 12
        scheduler = Scheduler(
            cron_freq_in_seconds=cron_freq_in_seconds,
            start=start,
            delta=delta,
            bin_size=bin_size,
        )

        real_handler = Handler(worker=worker, scheduler=scheduler)
        real_handler.handler(event, context)


.. _install:

Install
------------------------------------------------------------------------------

``dupe_remove`` is released on PyPI, so all you need is:

.. code-block:: console

    $ pip install dupe_remove

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade dupe_remove