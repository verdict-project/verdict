# Verdict
(Website: http://verdictdb.org/)

This is the new version 0.2.0. This readme file will be updated accordingly.

## 1. Introduction
Verdict makes database users able to get fast, approximate results for their aggregate queries on big data. 
It is designed to be a middleware standing between user's application and the DBMS.
Verdict gets the original query from user, transforms it and sends the new query(s) to the DBMS, and gets some raw results back. Then verdict calculates error estimates and return them along with the approximate answers to the user.
Verdict supports both uniform and stratified samples. It uses the bootstrap method for error estimation, enabling it to support  complex queries. For more information, refer to http://verdictdb.org/


<p align="center">
<img alt="Verdict's overview" src="https://github.com/mozafari/verdict/blob/master/docs/overview.png" width="600px" height="217px">
</p>



## 2. Supported DBMSs
Verdict is designed in a way that be as decoupled as possible from the underlying DBMS. The main parts of Verdict are independent from DBMS and just small amount of code needs to be added to support a new DBMS. This design lets us (or other developers in the future) to easily create a diver for any SQL DBMS and run Verdict on top of that DBMS. 

Currently we have developed drivers for: 

- **Spark SQL 1.1+**
- **Impala 2.3+**
- **Hive 1.2+**

To use Verdict with Presto, SAP HANA, HP Vertica, Oracle, MySQL, Postgres, Teradata, MemSQL or any other SQL engine please contact us to receive the free driver (pyongjoo [at] umich.edu). 


## 3. Getting Started

### 3.1. Requirements
Before you can install and run Verdict, the following requirements should be installed:
- JDK 1.8+ (Please make sure that $JAVA_HOME is set.)
- One of [the supported DBMSs](#2-supported-dbmss)

### 3.2. Installation
To install verdict you need to first clone the repository and build the project using SBT as follows. (

```
git clone https://github.com/mozafari/verdict.git
cd verdict
build/sbt assembly
```

Now you need to configure Verdict. In the [Configuration](#33-configuration) section, please read the part related to the DBMS you plan to use Verdict with.
 
### 3.3. Configuration
You need to configure Verdict before being able to run it. templates for Verdict's configurations can be found in the `configs` folder. Please find the template based on the DBMS you want to use and edit it based on description provided in the following subsections. 

#### Configurations for Impala 

Please replace the the values of the following configs in the `configs/impala.conf` file, if needed:

|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`dbms`         |None           |Tells Verdict what DBMS it should connect to. Use value `impala`.|
|`impala.host`  |`127.0.0.1`    |Impala's host address|
|`impala.port`  |`21050`        |Impala's JDBC port address|
|`impala.user`  |`""`           |Username to login with, if Impala's authentication is enabled|
|`impala.password`  |`""`       |Password to login with, if Impala's authentication is enabled|
|`udf_bin_hdfs` |None           |Verdict needs to install some UDF and UDAs on Impala. You need to copy the `udf_bin` folder to a location accessible by Impala in HDFS. Put the full HDFS path of `udf_bin` as the value for this config.|

Verdict for Impala also needs to connect to Hive. Because Impala hasn't necessary features for creating samples yet, Verdict uses Hive to create samples. Since Impala needs Hive to be running anyway and it uses Hive's metadata, Verdict's dependency to Hive is not a problem at all.

For using Verdict on Impala, you also need to set the values for Hive's configs. Please correct the Hive's config in `configs/impala.conf` based on the next section. Not that you do not need to edit `configs/hive.conf` for running verdict on Impala.

#### Configurations for Hive 

Please replace the values of the following configs in the `configs/hive.conf` file, if needed:

|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`dbms`         |None           |Tells Verdict what DBMS it should connect to. Use value `hive` if you want use Verdict on Hive. Don't set this to `hive` if you want use Verdict on Impala.|
|`hive.host`  |`127.0.0.1`    |Hive's host address|
|`hive.port`  |`10000`        |Hive's JDBC port address|
|`hive.user`  |`hive`           |Username to login with|
|`hive.password`  |`""`       |Password to login with|
|`udf_bin` |None           |Verdict needs to deploy some UDF and UDAs into Hive. You need to copy the `udf_bin` folder to a place accessible by Hive (If Hive is running in another server you may need to copy `udf_bin` folder to that server).|


#### Configurations for Spark SQL

Verdict connects to Spark SQL using Thrift JDBC server. You can find the instructions to run Thrift JDBC/ODBC server [here](https://spark.apache.org/docs/latest/sql-programming-guide.html#distributed-sql-engine).

Once you run Thrift JDBC server (and it is working with Beeline), please replace the values of the following configs in the `configs/sparksql.conf` file, if needed:

|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`dbms`         |None           |Tells Verdict what DBMS it should connect to. Use value `sparksql` if you want use Verdict on Spark SQL.|
|`sparksql.host`  |`127.0.0.1`    |Thrift's host address|
|`sparksql.port`  |`10001`        |Thrift's port address|
|`sparksql.user`  |`""`           |Username to login with|
|`sparksql.password`  |`""`       |Password to login with|
|`sparksql.connection_string_params`  |`;transportMode=http;httpPath=cliservice`       |These are the parameters Verdict will add to the connection string to connect to Thrift JDBC server. If you are using HTTP transport mode in Thrift JDBC server, keep it as is. Otherwise, replace the value with `""`.|
|`udf_bin` |None           |Verdict needs to deploy some UDF and UDAs into Spark SQL. You need to copy the `udf_bin` folder to a place accessible by Spark master (If Spark master is running in another server you may need to copy `udf_bin` folder to that server).|


### 3.4. Running Verdict

After building and configuring Verdict, you can run the its command line interface (CLI) using the following command:

`bin/verdict-cli -conf <config_file>`

Replace `<config_file>` with the config file you edited in the configuration step (i.e. `configs/impala.conf`, `config/hive.conf`, etc.)

You should be able to see the message `Successfully connected to <DBMS>`.



## 4. Using Verdict

Before you can [run approximate queries](#43-submitting-query), you need to do two things: [create sample(s)](#41-samples) and decide on [approximation options](#42-approximation-options)

### 4.1. Samples

Verdict uses sample for approximate queries. You should create the samples you need using the `CREATE SAMPLE` command:
 
```
CREATE SAMPLE <sample_name> FROM <table_name> WITH SIZE <size_percentage>% 
    [STORE <number_of_poisson_columns> POISSON COLUMNS] 
    [STRATIFIED BY <column(s)>];
```

|Argument                 |Description                                        |
------------------------|------------                                       |
|`<sample_name>`        |The name of the sample to be created|
|`<table_name>`         |The name of the original table|
|`<size_percentage>%`   |The size of sample relative to the original sample, e.g. `5%`|
|`<number_of_poisson_columns>`   |This part is optional. This option specifies the number of Poisson random number columns to be generated and stored in the sample. These random numbers are needed only when you are using the `stored` bootstrap method.|
|`<column(s)>`   |If you want to create a uniform sample just ignore this part, otherwise, if you want to create a stratified sample, with this option you can specify the column(s) based on which Verdict should construct strata. The resulting sample will have a stratum for each distinct value of the specified column(s).|


To list the existing samples use the following command:

```
SHOW [<type>] SAMPLES [FOR <table_name>];
```

|Argument                 |Description                                        |
------------------------|------------                                       |
|`<type>`        |This is an optional argument that can be one of `ALL`, `STRATIFIED` or `UNIFORM` to specify the type of samples to be listed.|
|`<table_name>`         |An optional argument, can be used to list just samples of an specific table. If not specified, Verdict will list the samples for all tables.|

To delete a sample use the following command:

```
DROP SAMPLE <sample_name>;
```

### 4.2. Approximation Options

The following options tells Verdict how to process approximate queries. You can customize these approximation options based on your needs, but for the most part, the default values are a reasonable choice for most users.
The default values of these options can be specified in the config file. You can also re-set these values before each query while Verdict is running using the [`SET`](#45-set-and-get-commands) command.
 
|Config         |Default Value  |Description                                        |
|------         |-------------  |-----------                                        |
|`approximation`    |`inline`           |This option can be set to `auto` or `inline`. In `auto` mode Verdict will process all incoming queries and will automatically transform all eligible queries to approximate queries. However, in `inline` mode, Verdict will only considers queries with `SAMPLE` or `CONFIDENCE` clause as approximate queries and won't touch regular queries.
|`sample_size`|`1%`   |A percentage that determines the preferred size for the sample (relative to the actual table) used for running approximate query. Choosing a small sample makes your queries faster but with higher error. When multiple samples are present for a table, Verdict tries to use the sample which size is closest to this value.|
|`sample_type`|`uniform`   |This option tells Verdict what kind of sample (`uniform` or `stratified`) do you prefer to run your query on. If both kind of samples are present for a table, Verdict tries to chose the one that is the kind specified in this option.|
|`error_columns`|`conf_inv`     | This options tells Verdict to generate what extra columns for error estimation in the query results. You can specify any combination of the following: confidence intervals (`conf_inv`), estimated absolute error bound (`abs_err`), estimated relative error bound (`rel_err`) and variance (`variance`). To specify more than one, seperate them with comma, for example using value `conf_inv, err_percent` will generate two more columns for each aggregate expression in the result set, one for confidence intervals and one for error bound percentages.
|`confidence`|`95%`   |A percentage that determines the confidence level for reporting the confidence interval (error estimation). For example when it is set to 95%, it means that Verdict is 95% confident that the true answer for the query is in the provided bound.|
|`bootstrap.method`    |`uda`   |This option can have one of the values `uda`,`udf` or `stored`. It determines the method Verdict uses to perform bootstrap trials for calculating estimated error. Usually the `uda` method is the fastest, but other two options are useful for the DBMSs that don't support UDA (user defined aggregate function).|
|`bootstrap.trials`    |`50`   |An integer specifying the number of bootstrap trials being run for calculating error estimation. Usually `50` or `100` work well. Choosing a very small number reduces the accuracy of error estimations, while a very large number of bootstrap trials makes thee query slow.|


### 4.3. Submitting Query

After creating the proper samples, you can submit your queries. You should write your query as you would for an exact answer, that is, you should use the original table in your query, not a sample.

For example, one query can be the following, where the `sales` is the name of the original table:

```
SELECT department, SUM(price) as revenue from sales GROUP BY department;
```

On an exact DBMS you'll get a result like below:

```
department  |revenue 
--------------------
foo         |4893
bar         |34509
```

However Verdict will output an answer like:

```
department  |revenue    |conf_inv_lower__revenue | conf_inv_upper_revenue   
-------------------------------------------------------------------------
foo         |4848       |4753                   |5023
bar         |34613      |34332                  |34690
```

`conf_inv_lower__revenue` and `conf_inv_lower__revenue` are the confidence interval upper and lower bound for the approximate values in the second column (`revenue`). That means, for example, for department foo the approximate revenue is $4848 and Verdict is 95% confident that the actual revenue is between $4753 and $5023.

If there are more aggregate function in the query, there will be a column for the confidence interval of each aggregation at the end.
 


### 4.4. Supported Queries

Currently, Verdict supports the queries that have the following criteria:
- Query should have at least one of the supported aggregate functions `COUNT`, `SUM` and `AVG`
- Query can have sub-queries, but the aggregate functions cannot be in sub-queries.
- Query can have JOINs, but Verdict will replace one of the tables with a sample.

If Verdict identify a query as unsupported query, it will try running the query without modification. 


### 4.5. `SET` and `GET` commands

You can use `SET` and `GET` commands to set or get the value of a [approximation option](#42-approximation-options) while verdict is running.

```
SET <parameter> = <value>;
GET <parameter>;
```

#### Example
```
> SET sample_size = 1%;
> GET sample_type;
```


