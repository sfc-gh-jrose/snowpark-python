# flake8: noqa
from tests.parameters import *
from snowflake.snowpark import Session
import snowflake.snowpark.functions as f
from snowflake.snowpark.types import *

params = CONNECTION_PARAMETERS
session = Session.builder.configs(params).create()

# Read from kafka
df = (
    session.read_stream.format("kafka")
    .option("key_path", "~/.ssh/rsa_key.p8")
    .option("table_name", "jrose_test_table")
    .option("pipe_name", "jrose_test_pipe")
    .option("subscribe", "topic")
    .load()
)
print("Stream Reader configured.")

# Transform Data
split = df.select(f.split(f.col("value"), f.lit(" ")).alias("split"))
df2 = split.select(f.explode("split").alias("word")).groupBy("word").count()

print("Transformation described.")

# Write to dynamic table
print("Stream starting.")
(df2.write_stream.format("table").option("table", "jrose_test_dynamic_table").start())
print("Test Complete")
