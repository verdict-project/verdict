# Connecting to Data Sources

## Supported Databases

- MySQL 5.5 or later
- PostgreSQL 10 or later
- Amazon Redshift
- Impala 2.5 (CDH 5.7) or later
- Spark 2.0 or later


The following databases will be supported soon:

- Hive
- Oracle
- SQL Server
- Presto


## Connection Options

1. **Standard JDBC Interface**: One can issue queries to and retrieve the results from VerdictDB using the standard JDBC interface. This approach is applicable for all databases that support the JDBC interface. For VerdictDB to retrieve data from the backend database, VerdictDB requires the connection information to the backend database. This connection information can be specified by either of the two ways, as follows.
    - **JDBC string**: Passing a modified JDBC string to the standard Java DriverManager. See database-specific examples below.
    - **JDBC connection**: Passing an already established JDBC connection (to the backend DB) to VerdictDB. See database-specific examples below.
1. **VerdictContext**: One can also connect to VerdictDB directly using its own interface called VerdictContext. An instance of VerdictContext can be created either using the JDBC connection information or using an instance of SparkSession. The query results returned from VerdictContext use ?? for convenient operations.



## MySQL

### JDBC string

See that the `verdict` keyword is inserted to the regular JDBC connection string for the MySQL connection.

```java
String connectionString =
    String.format("jdbc:verdict:mysql://%s:%d/%s",
        MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE);
Connection vc = DriverManager.getConnection(connectionString, MYSQL_UESR, MYSQL_PASSWORD);
```

### JDBC connection

*(Yongjoo: fill this in)*



## PostgreSQL

### JDBC string

```java
// use JDBC connection URL as connection string
String connectionString =
        String.format("jdbc:verdict:postgresql://%s:%d/%s?user=%s&password=%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE);
Connection vc = DriverManager.getConnection(connectionString, POSTGRES_USER, POSTGRES_PASSWORD);
```

### JDBC connection

*(Yongjoo: fill this in)*



## Redshift

### JDBC string

```java
// use JDBC connection URL as connection string
String connectionString =
        String.format("jdbc:verdict:redshift://%s:%d/%s", REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE);
Connection vc = DriverManager.getConnection(connectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
```

### JDBC connection

*(Yongjoo: fill this in)*


## Cloudera Impala

### JDBC string

```java
// use JDBC connection URL as connection string
String impalaConnectionString =
        String.format("jdbc:verdict:impala://%s:%d/%s", IMPALA_HOST, IMPALA_PORT, IMPALA_DATABASE);
Connection vc = DriverManager.getConnection(impalaConnectionString, IMPALA_USER, IMPALA_PASSWORD);
```

### JDBC connection

*(Yongjoo: fill this in)*


## Apache Spark

### Spark Session

*(Yongjoo will write this later)*
