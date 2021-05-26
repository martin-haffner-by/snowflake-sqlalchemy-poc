from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from snowflake.sqlalchemy import CreateFileFormat, PARQUETFormatter

"""
This demonstrates compiling a Snowflake-specific SQL statement to text, using the
classes in the Snowflake/SQLAlchemy dialect.
While the Snowflake/SQLAlchey dialect has to be installed, an actual connection to
Snowflake is not required.
"""
create_format = CreateFileFormat(
    format_name="ML_POC.PUBLIC.PARQUET_FILE_FORMAT",
    formatter=PARQUETFormatter().compression("AUTO").binary_as_text(True)
)

create_format_sql = create_format.compile(dialect=SnowflakeDialect())

"""
this yields the following string:
CREATE OR REPLACE FILE FORMAT ML_POC.PUBLIC.PARQUET_FILE_FORMAT TYPE='parquet' 
COMPRESSION = 'AUTO' BINARY_AS_TEXT = true
"""
print(create_format_sql)
