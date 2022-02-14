# Analytics Engineering & DBT

### Analytics Engineering

Anaytics Engineering was brought in to bridge the gap between Data Engineering and Data Analysts/Scientists.

#### Roles in a Data Team
-   Data Engineer: Prepares and maintains the infrastructure the data team needs.
-   ***Analytics Engineer:***  Introduces the good software engineering practices to reduce the efforts of Data Analysts/Scientists as they are not expected to be proficient software engineers.
-   Data Analyst: Uses data to answer questions and solve business problems.

#### Tooling

- Data Loading: Use tools like Fivetran, Stitch to load data
- Data Storing: Cloud data warehouses like Snowflake, BigQuery, Redshift
- Data Modelling: Tools like DBT or Dataform
- Data Presentation: BI tools like Looker, Data Studio, Mode or Tableau.

### Data Modelling Concepts

#### ETL vs ELT

- **ETL**: 
  - Extract data from source, transform it and then load it to the data warehouse
  - Takes longer are we need to build transformation processes and dimensional models before loading.
  - The data is cleaner and more stable
- **ELT**:
  - Tranforms the data after it is loaded into the data lake or warehouse.
  - It is faster as the data is quickly available and the individual teams can start working on it
  - Can use the cloud data warehousing capabilities to transform data and lower the compute costs.

#### Elements of Dimensional Models

**Fact Tables**
- Measurements, metrics or facts about our business
- They correspond to a business process
- *"verbs"*

**Dimension Tables**
- Corresponds to a business Entity
- Provides context to a business process
- *"nouns"*

### DBT

#### What is DBT?
DBT is a transformational tool that allows anyone that knows SQL to deploy analytics code following software engineering best practices like modularity, portability, CI/CD and documentation.  

#### Setup DBT

Use this [guide](docker-setup.md) to setup dbt with Big Query on Docker.

#### Configuring Profile

[Official Docs On Profile Configuration](https://docs.getdbt.com/dbt-cli/configure-your-profile)

When you invoke dbt from the command line, dbt parses your `dbt_project.yml` and obtains the `profile` name, which dbt needs to connect to your data warehouse.

dbt then checks your `profiles.yml` file for a profile with the same name. A profile contains all the details required to connect to your data warehouse.

This file generally lives outside of your dbt project to avoid sensitive credentials being check in to version control. By default, dbt expects the `profiles.yml` file to be located in the `~/.dbt/` directory.

#### Anatomy of a dbt model

Materialization Strategies:
- Table
- View
- Incremental
- Ephemeral

```sql
{{
  config(materialized='table')
}}

select * 
from staging.source_table
where record_state='ACTIVE' 
```

Compiled code would look something like this:

```sql
create table my_schema.my_model as (
  Select *
  from staging.source_table
  where record_state='Active'
)
```