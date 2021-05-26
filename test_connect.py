#!/usr/bin/env python
import snowflake.connector
import os
from parameters import CONNECTION_PARAMETERS_SNOWFLAKE

ctx = snowflake.connector.connect(
    user=CONNECTION_PARAMETERS_SNOWFLAKE["user"],
    password=CONNECTION_PARAMETERS_SNOWFLAKE["password"],
    account=CONNECTION_PARAMETERS_SNOWFLAKE["account"],
    )
cs = ctx.cursor()
cs.execute("SELECT current_version()")
one_row = cs.fetchone()
print(one_row[0])
