# -*- coding: utf-8 -*-

from sqlalchemy import MetaData, text
from sqlalchemy_mate import EngineCreator

# engine_creator = EngineCreator.from_s3_json(
#     bucket_name="login.gov-dev-sanhe",
#     key="db/redshift-analytics-dev.json",
#     aws_profile="identitysandbox.gov",
# )

engine_creator=EngineCreator(
    host="analytics-dev.caddas6ru6aa.us-west-2.redshift.amazonaws.com",
    port=5439,
    database="dev",
    # username="awsuser",
    # password="5gB&1aCrt1Ap",
    username="sanhehu",
    password="Sanhe1987",
)
engine = engine_creator.create_redshift()

metadata = MetaData()
metadata.reflect(engine)
print(metadata.tables)

print(engine.execute(text("SELECT COUNT(*) FROM events")).fetchone())