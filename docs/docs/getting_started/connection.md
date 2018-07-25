# Connecting to Data Sources

## Supported Databases

- CDH 5.7 / Impala 2.5 or later
- Spark 2.0 or later
- MySQL 5.5 or later
- Redshift
- PostgreSQL 10 or later

TODO:

- Hive
- Oracle
- SQL Server



## MySQL
```java
// use JDBC connection URL as connection string
String mysqlConnectionString =
        String.format("jdbc:verdict:mysql://%s:%d/%s", MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE);
Connection vc = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
```

## PostgreSQL

```java
// use JDBC connection URL as connection string
String postgreSQLConnectionString =
        String.format("jdbc:verdict:postgresql://%s:%d/%s?user=%s&password=%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DATABASE);
Connection vc = DriverManager.getConnection(postgreSQLConnectionString, POSTGRES_USER, POSTGRES_PASSWORD);
```

## Apache Spark

*(Yongjoo will write this later)*

## Redshift

```java
// use JDBC connection URL as connection string
String redshiftConnectionString =
        String.format("jdbc:verdict:redshift://%s:%d/%s", REDSHIFT_HOST, REDSHIFT_PORT, REDSHIFT_DATABASE);
Connection vc = DriverManager.getConnection(redshiftConnectionString, REDSHIFT_USER, REDSHIFT_PASSWORD);
```

## Cloudera Impala

```java
// use JDBC connection URL as connection string
String impalaConnectionString =
        String.format("jdbc:verdict:impala://%s:%d/%s", IMPALA_HOST, IMPALA_PORT, IMPALA_DATABASE);
Connection vc = DriverManager.getConnection(impalaConnectionString, IMPALA_USER, IMPALA_PASSWORD);
```

