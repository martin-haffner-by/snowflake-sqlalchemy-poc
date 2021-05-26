# snowflake-sqlalchemy-poc

This repo contains some POCs on how to access Snowflake using SQLAlchemy and / or
Kartothek.

This POC relies on the latest version of the snowflake-sqlalchemy dialect, which may
not yet be officially released. To include it, clone the repo from
https://github.com/martin-haffner-by/snowflake-sqlalchemy, build the wheel according
to the instructions and install the library.

To run against a real Snowflake installation, create a file ``parameters.py`` with the
structure below. *DO NOT ADD THE parameters.py FILE TO THE REPOSITORY*.

```
CONNECTION_PARAMETERS_SNOWFLAKE = {
    'account': 'your_account.your_region.your_cloud_provider',
    'user': 'your_user_name',
    'password': 'your_password',
    'schema': 'your_schema',
    'database': 'your_database',
    'warehouse': 'your_warehouse',
}

CONNECTION_PARAMETERS_AZURE = {
    'account': 'your account',
    'container': 'your container',
    'sas_token_urldecoded': 'your token',
}
```
Specify the token in unquoted form: `se=...&sp=...&sv=...&sr=...&sig=...`
