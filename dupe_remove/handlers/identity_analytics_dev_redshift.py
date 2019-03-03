# -*- coding: utf-8 -*-


def handler(event, context):
    from sqlalchemy_mate import EngineCreator
    from datetime import datetime, timedelta
    from dupe_remove.tests import table_name, id_col_name, sort_col_name
    from dupe_remove import Scheduler, Worker, Handler

    engine_creator = EngineCreator.from_s3_json(
        bucket_name="login.gov-dev-sanhe",
        key="db/redshift-analytics-dev.json",
    )
    engine = engine_creator.create_redshift()

    cron_freq_in_seconds = 300
    start = datetime(2018, 1, 1)
    delta = timedelta(days=31)
    bin_size = 12
    scheduler = Scheduler(cron_freq_in_seconds=cron_freq_in_seconds,
                          start=start, delta=delta, bin_size=bin_size)
    worker = Worker(engine=engine,
                    table_name=table_name,
                    id_col_name=id_col_name,
                    sort_col_name=sort_col_name)
    real_handler = Handler(scheduler, worker)
    real_handler.handler(event, context)
