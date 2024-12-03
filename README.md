**Welcome to the Dataiku dbt Runner Demo Plugin!**

This demo plugin showcases the integration of Dataiku, dbt, and Snowflake, highlighting key functionalities related to workflow orchestration and automation.

Currently, the plugin supports **Snowflake**, but in theory, the same approach could be adapted to work with other databases supported by dbt.

## Requirements
In order to successfully run this project, you will need the following:

- A **Snowflake account**: If you have your own dbt project, you should be familiar with the necessary permissions to run dbt projects..
- A **Dataiku instance** where this plugin can be installed.
- **Snowflake Sample Data** [The TPC-H dataset that is standard with every account](https://docs.snowflake.com/en/user-guide/sample-data-tpch.html)
- **Snowflake warehouse** named MEDIUM_WH (or update the dbt project to use your own warehouse)

## How to:
Once you have installed this plugin on your Dataiku instance, you can use the macro called **"dbt runner"** inside any project. This macro requires the following three mandatory parameters::
- **Git repo url**: suggest using [the dbt-cloud-snowflake-demo-template git project](https://github.com/johnson-zhang-au/dbt-cloud-snowflake-demo.git), which is a fork of [the dbt Labs Snowflake Demo Project](https://github.com/dbt-labs/dbt-cloud-snowflake-demo-template), or clone it to you own repo
- **Git branch name**: The default is the main branch

- **Choose a connection from**: Select how you want to specify the database connection:
  - **Manual Input**: Enter the database connection name manually as defined on this instance. Ensure the connection already exists.
  - **Retrieve Available Snowflake Connections at the Instance Level**: This option lists all Snowflake connections available on the instance but requires platform admin permissions.
  - **Retrieve Available Snowflake Connections for This Project**: The macro attempts to identify all Snowflake connections accessible within the current project. This includes connections associated with datasets in the project. Your account must have write access to the project. Note: Certain account profiles, such as *Reader*, *AI Consumer*, or *Governance Manager*, do not have the necessary permissions for this operation.
  _The permissions mentioned here are only for automatically retrieve avaliable connections, they are not required to run the macro after you selected the correct database connection_
- **Database connection name**:  The macro will automatically retrieve the following authentication parameters from the Snowflake connection:
    - Snowflake hostname
    - Snowflake database name
    - Snowflake schema name
    - Snowflake username
    - Snowflake password
    - Snowflake access token (if OAuth is used)
    - OAuth app id (if OAuth is used)
    - OAuth app secret (if OAuth is used)
< The Macro gives you the options to:

>
Additionally, for Snowflake OAuth with per-user credentials, you will need:
- Snowflake user (for OAuth only)

## Snowflake permissions for the dbt-cloud-snowflake-demo-template dbt project

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
