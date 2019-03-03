# -*- coding: utf-8 -*-

"""
docker run --rm --name dupe-remove-test-db -p 5432:5432 -e POSTGRES_PASSWORD=password -d postgres
docker container stop dupe-remove-test-db
"""

from sqlalchemy_mate import EngineCreator

ec = EngineCreator(host="0.0.0.0", port=5432, database="postgres",
                   username="postgres", password="password")

engine = ec.create_postgresql_psycopg2()
