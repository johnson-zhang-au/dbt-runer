Welcome to the Dataiku dbt runner Demo plugin!

This dbt runner plugin project is intended to showcase key Dataiku + dbt + Snowflake functionalities in terms of workflow, orchastration.

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
