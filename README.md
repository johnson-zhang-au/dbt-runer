**Welcome to the Dataiku dbt Runner Demo Plugin!**

This demo plugin showcases the integration of Dataiku, dbt, and Snowflake, highlighting key functionalities related to workflow orchestration and automation.

Currently, the plugin supports **Snowflake**, but in theory, the same approach could be adapted to work with other databases supported by dbt.

## Requirements
In order to successfully run this project, you will need the following:

- A Snowflake account: If you have your own dbt project, you should be familiar with the necessary permissions to run dbt projects..
- A Dataiku instance that you can install this plugin
- Snowflake Sample Data [The TPC-H dataset that is standard with every account](https://docs.snowflake.com/en/user-guide/sample-data-tpch.html)
- Snowflake warehouse named MEDIUM_WH (or update the dbt project to use your own warehouse)

## How to:
After installing this plugin to your Dataiku instance, you will have a macro called "dbt runer", this macro has three mandatory parameters:
- Git repo url: suggest using [the dbt-cloud-snowflake-demo-template git project](https://github.com/johnson-zhang-au/dbt-cloud-snowflake-demo.git), which is a fork of [the dbt Labs Snowflake Demo Project](https://github.com/dbt-labs/dbt-cloud-snowflake-demo-template), or clone it to you own
- Git branch name: default is the main branch
- Connection name to the database : the macro will automatically pick up the following authentication parameters from the snowflake connection:
    - Snowflake hostname
    - Snowflake database name
    - Snowflake schema name
    - Snowflake username
    - Snowflake password
    - Snowflake access token (if OAuth is used)
    - OAuth app id (if OAuth is used)
    - OAuth app secret (if OAuth is used)

And one for Snowflake OAuth if per user credential is used:
- Snowflake user (for OAuth only)

## Snowflake permissions to use the dbt-cloud-snowflake-demo-template dbt project

This dbt project creates a staging schema by appending the "_staging" suffix to the schema name specified in your Dataiku Snowflake connection.. Therefore, your Snowflake account must have the necessary permissions to create new schemas under the specified database in your Snowflake connection.

The project also attempts to grant permissions to the "PUBLIC" role after data is loaded into the target tables, as shown below:

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

Therefore, your account will need sufficient privileges to execute these actions. If your account is the schema owner, the following additional permissions are required:

```sql
GRANT MANAGE GRANTS ON DATABASE <<YOUR SF DATABASE ON DKU SNOWFLAKE CONNECTION>> TO ROLE <<YOUR SF ROLE ON DKU SNOWFLAKE CONNECTION>>;

GRANT MANAGE GRANTS ON ACCOUNT TO ROLE <<YOUR SF ROLE ON DKU SNOWFLAKE CONNECTION>>;
```
