from kartothek.serialization.testing import get_dataframe_not_nested
from kartothek.io.eager import store_dataframes_as_dataset
from kartothek.core.dataset import DatasetMetadata
from functools import partial
import storefact
from parameters import CONNECTION_PARAMETERS_SNOWFLAKE, CONNECTION_PARAMETERS_AZURE
from sqlalchemy import create_engine, Table, MetaData, Column, Boolean, select, text, Integer, Float, String, Date, DateTime, Unicode
from snowflake.sqlalchemy import URL
from sqlalchemy.schema import CreateTable
from snowflake.sqlalchemy import CopyIntoStorage, ExternalStage, CreateStage, AzureContainer, CopyFormatter
from datetime import datetime
import pandas as pd
import urllib

"""
This demonstrates ingesting a Kartothek (KTK) file into a Snowflake table.
Note on prerequisites:
- add a local file parameters.py which contains credentials for Azure and Snowflake.
  DO NOT add this file to the git repository.
- include latest version of SQLAlchemy for Snowflake. (1.2.5). If not yet available
  using pip and not yet whitelisted, clone the git repo and build & install it
  manually.
"""

# create test Dataframe, consisting of all kinds of KTK column types
def _create_df(df=None, partition_cols=None):
    # Create dataset on local filesystem
    if df is None:
        df = get_dataframe_not_nested(100)
    if partition_cols:
        for partition_col, partition_from in partition_cols.items():
            source_col = partition_from["source_col"]
            cast_as = partition_from["cast_as"]
            # create partitioning column and cast if require
            df[partition_col] = df[source_col].apply(
                lambda x: cast_as and cast_as(x) or x
            )
    return df


df = _create_df()

# rename the columns , just to make sure that no problems occur
# because columns have names like "datetime" which represent data types
df = df.rename(columns={
    "date": "date_col", "null": "null_col", "bool": "bool_col", "bytes": "bytes_col",
    "datetime64": "datetime64_col", "float32": "float32_col", "float64": "float64_col",
    "int16": "int16_col", "int32": "int32_col", "int64": "int64_col",
    "int8": "int8_col", "uint16": "uint16_col", "uint32": "uint32_col",
    "uint64": "uint64_col", "uint8": "uint8_col", "unicode": "unicode_col",
})

# modify the datetime column to include a time stamp; and add a string column with
# real textual data
df["datetime64_col"] = pd.Series(
                [datetime(2018, 1, x % 31 + 1, x % 24, x%24, 0) for x in range(1, 100 + 1)],
                dtype="datetime64[ns]")
df["string_col"] = pd.Series(
                ["foo" if x % 2 else "bar" for x in range(1, 101)], dtype="str")

dfs = [df, df]

# write this dataframe to an Azure Blob storage as KTK dataset
uuid = "test_dataset_for_snowflake"
url = f"hazure://{CONNECTION_PARAMETERS_AZURE['account']}:" \
      f"{urllib.parse.quote_plus(CONNECTION_PARAMETERS_AZURE['sas_token_urldecoded'])}@" \
      f"{CONNECTION_PARAMETERS_AZURE['container']}?use_sas=true&create_if_missing=false"
store_factory = partial(storefact.get_store_from_url, url)
if not DatasetMetadata.exists(uuid=uuid, store=store_factory):
    store_dataframes_as_dataset(
        store=store_factory,
        dataset_uuid=uuid,
        dfs=dfs,
    )

# auto-detect the first existing Parquet file from this store, and ingest from this file
md = DatasetMetadata.load_from_store(store=store_factory,
                                     uuid=uuid, load_schema=True, load_all_indices=True)
table_to_import = md.tables[0]
partition_to_import = list(md.partitions.keys())[0]


# connect to Snowflake
# Note: Schema would also be specified here in a standard case. Since this use
# case uses a dummy test schema which can be dropped after running, the USE SCHEMA
# is executed separately.
engine = create_engine(URL(
        user=CONNECTION_PARAMETERS_SNOWFLAKE["user"],
        password=CONNECTION_PARAMETERS_SNOWFLAKE["password"],
        account=CONNECTION_PARAMETERS_SNOWFLAKE["account"],
        database=CONNECTION_PARAMETERS_SNOWFLAKE["database"],
        warehouse=CONNECTION_PARAMETERS_SNOWFLAKE["warehouse"]
    )
)
try:
    connection = engine.connect()

    # Uncomment if you want to run on a clean schema
    # connection.execute(f'DROP SCHEMA IF EXISTS {CONNECTION_PARAMETERS_SNOWFLAKE["schema"]}')

    # Note: As far as I see, there is no SQLAlchemy object yet to express the "if not
    # exists", therefore this is hard-coded here.
    connection.execute(f'CREATE SCHEMA IF NOT EXISTS {CONNECTION_PARAMETERS_SNOWFLAKE["schema"]}')
    connection.execute(f'USE SCHEMA {CONNECTION_PARAMETERS_SNOWFLAKE["schema"]}')

    # create a new table
    metadata = MetaData()
    new_table = Table(
        "KTK_TYPE_CHECK",
        metadata,
        Column("bool_col", Boolean),
        Column("bytes_col", String),
        Column("date_col", Date),
        Column("datetime64_col", DateTime),
        Column("float32_col", Float),
        Column("float64_col", Float),
        Column("int8_col", Integer),
        Column("int16_col", Integer),
        Column("int32_col", Integer),
        Column("int64_col", Integer),
        Column("null_col", Integer),
        Column("uint8_col", Integer),
        Column("uint16_col", Integer),
        Column("uint32_col", Integer),
        Column("uint64_col", Integer),
        Column("unicode_code", Unicode),
        Column("string_col", String),
    )
    connection.execute(CreateTable(new_table))

    # create a new named stage, located in an Azure container
    root_stage = ExternalStage(
        name="KTK_POC_STAGE",
        namespace=f"{CONNECTION_PARAMETERS_SNOWFLAKE['database']}.{CONNECTION_PARAMETERS_SNOWFLAKE['schema']}",
    )
    container = AzureContainer(
        account=CONNECTION_PARAMETERS_AZURE['account'],
        container=CONNECTION_PARAMETERS_AZURE['container']
    ).credentials(CONNECTION_PARAMETERS_AZURE['sas_token_urldecoded'])
    create_stage = CreateStage(stage=root_stage, container=container)
    connection.execute(create_stage)

    # define the Select statement to read from parquet. Note that all columns of the
    # file will be consolidated under one column ($1), but can still be accessed using
    # their name.
    # NOTE ON TIMESTAMP format: KTK stores datetimes in microseconds. Snowflake does
    # not seem to be able to auto-detect this, so an explicit conversion is necessary.
    sel_statement = select(
        text("$1:bool_col::boolean"),
        text("$1:bytes_col::string"),
        text("$1:date_col::date"),
        text("TO_TIMESTAMP_NTZ($1:datetime64_col::integer/1000000)"),
        text("$1:float32_col::float"),
        text("$1:float64_col::float"),
        text("$1:int8_col::integer"),
        text("$1:int16_col::integer"),
        text("$1:int32_col::integer"),
        text("$1:int64_col::integer"),
        text("$1:null_col::integer"),
        text("$1:uint8_col::integer"),
        text("$1:uint16_col::integer"),
        text("$1:uint32_col::integer"),
        text("$1:uint64_col::integer"),
        text("$1:unicode_col::string"),
        text("$1:string_col::string"),
    ).select_from(
        ExternalStage.from_root_stage(
            root_stage,
            f"test_dataset_for_snowflake/{table_to_import}/{partition_to_import}.parquet"
        ))

    # run a CopyInto job using the Select statement, the target table and a formatter.
    # (Note: Named format "parquet_file_format" does already exist in the PUBLIC
    # schema, so this can be used without being created)
    formatter = CopyFormatter(format_name="parquet_file_format")
    copy_into = CopyIntoStorage(
        from_=sel_statement,
        into=new_table,
        formatter=formatter
    )
    connection.execute(copy_into)
    # Uncomment if you want to clean up after finishing
    # connection.execute(f'DROP SCHEMA IF EXISTS {CONNECTION_PARAMETERS_SNOWFLAKE["schema"]}')

finally:
    connection.close()
    engine.dispose()

"""
Note: verify results in Snowflake Web UI:
- select * from KTK_TYPE_CHECK LIMIT 10;
- Use command history to view which commands did run
"""
