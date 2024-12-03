*Welcome to the Dataiku dbt Runner Demo Plugin!*

This demo plugin showcases the integration of Dataiku, dbt, and Snowflake, highlighting key functionalities related to workflow orchestration and automation.

Currently, the plugin supports *Snowflake*, but in theory, the same approach could be adapted to work with other databases supported by dbt.

## Requirements
In order to successfully run this project, you will need the following:

- A Snowflake account: if you have your own dbt project, then you should know what permissions are required to run your dbt project.
- A Dataiku instance that you can install this plugin
- Snowflake Sample Data [The TPC-H dataset that is standard with every account](https://docs.snowflake.com/en/user-guide/sample-data-tpch.html)
- Snowflake warehouse named MEDIUM_WH (or update the dbt project to use your own warehouse)

## How to:
After installing this plugin to your Dataiku instance, you will have a macro called "dbt runer", this macro has three mandatory parameters:
- Git repo url: suggest using [the dbt-cloud-snowflake-demo-template git project](https://github.com/johnson-zhang-au/dbt-cloud-snowflake-demo.git) or clone it to you own
- Git branch name: default is the main branch
- Connection name to the database : the macro will automatically pick up the following authentication parameters from the snowflake connection:
    - Snowflake hostname
    - Snowflake database name
    - snowflake schema name
    - snowflake username
    - Snowflake password
    - Snowflake access token (if OAuth is used)
    - OAuth app id (if OAuth is used)
    - OAuth app secret (if OAuth is used)

And one for Snowflake OAuth if per user credential is used:
- Snowflake user (for OAuth only)

## Snowflake permissions to use the dbt-cloud-snowflake-demo-template dbt project
In this project, it creates a stage schema named with "_staging" as the suffix with the schema in your Dataiku Snowflake connection, so your Snwoflake account need to have the permission to be able to create new schema under the database specified in your Snowflake connection.

The project also tries to grant permissions after the data is loaded to the target tables,
```yaml
on-run-end:
  - "{{ grant_all_on_schemas(schemas, 'public') }}"
```

```sql
{% macro grant_all_on_schemas(schemas, role) %}
  {% for schema in schemas %}
    grant usage on schema {{ schema }} to role {{ role }};
    grant select on all tables in schema {{ schema }} to role {{ role }};
    grant select on all views in schema {{ schema }} to role {{ role }};
    grant select on future tables in schema {{ schema }} to role {{ role }};
    grant select on future views in schema {{ schema }} to role {{ role }};
  {% endfor %}
{% endmacro %}
```

So your account will need to be able to do so. If your account is the owner of the schema, then you only need the following additional permissions:
```sql
GRANT MANAGE GRANTS ON DATABASE <<YOUR SF DATABASE ON DKU SNOWFLAKE CONNECTION>> TO ROLE <<YOUR SF ROLE ON DKU SNOWFLAKE CONNECTION>>;

GRANT MANAGE GRANTS ON ACCOUNT TO ROLE <<YOUR SF ROLE ON DKU SNOWFLAKE CONNECTION>>;
```
