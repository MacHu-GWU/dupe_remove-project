# -*- coding: utf-8 -*-


def handler(event, context):
    from sqlalchemy_mate import EngineCreator, test_connection
    from datetime import datetime, timedelta
    from dupe_remove.tests import table_name, id_col_name, sort_col_name
    from dupe_remove import Scheduler, Worker, Handler

    engine_creator = EngineCreator.from_env(prefix="DEV_DB", kms_decrypt=True)
    engine = engine_creator.create_redshift()
    test_connection(engine, 6)

    worker = Worker(
        engine=engine,
        table_name=table_name,
        id_col_name=id_col_name,
        sort_col_name=sort_col_name,
    )

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
