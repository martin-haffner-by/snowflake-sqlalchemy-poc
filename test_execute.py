from snowflake.sqlalchemy import CreateFileFormat, PARQUETFormatter
from parameters import CONNECTION_PARAMETERS_SNOWFLAKE
from sqlalchemy import create_engine
from snowflake.sqlalchemy import URL

TEST_SCHEMA = "__POC_SCHEMA_SNOWFLAKE_CONNECT"

"""
This demonstrates executing a Snowflake-specfific SQL statement directly against
the Snowflake engine.
Snowflake access is required, credentials to be found in parameters.py.
You can find the results using the Snowflake Web UI: 
SHOW FILE FORMATS IN SCHEMA ML_POC.__POC_SCHEMA_SNOWFLAKE_CONNECT
And you can verify which commands where executed using the Command History in the
Snowflake WebUI.
"""
engine = create_engine(URL(
        user=CONNECTION_PARAMETERS_SNOWFLAKE["user"],
        password=CONNECTION_PARAMETERS_SNOWFLAKE["password"],
        account=CONNECTION_PARAMETERS_SNOWFLAKE["account"],
        database=CONNECTION_PARAMETERS_SNOWFLAKE["database"],
    )
)
try:
    connection = engine.connect()

    # Note: As far as I see, there is no SQLAlchemy object yet to express the "if not
    # exists", therefore this is hard-coded here.
    connection.execute(f'CREATE SCHEMA IF NOT EXISTS {TEST_SCHEMA}')

    create_format = CreateFileFormat(
        format_name=f"ML_POC.{TEST_SCHEMA}.PARQUET_FILE_FORMAT",
        formatter=PARQUETFormatter().compression("AUTO").binary_as_text(True)
    )
    connection.execute(create_format)
    # Uncomment if you want to clean up before finishing
    # connection.execute(f'DROP SCHEMA IF EXISTS {TEST_SCHEMA}')
finally:
    connection.close()
    engine.dispose()
