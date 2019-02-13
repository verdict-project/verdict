# Connecting to Databases (Java)

VerdictDB can process the data stored in a SQL-supported database; therefore, no data migration is required outside your database. VerdictDB works with them by performing its operations in SQL.

## Supported Databases

<!-- - MySQL 5.5 or later
- PostgreSQL 10 or later
- Amazon Redshift
- Impala 2.5 (CDH 5.7) or later
- Presto (Hive-Connector)
- Spark 2.0 or later -->

| Database        | VerdictDB                 | PyVerdict                 |
|-----------------|---------------------------|---------------------------|
| MySQL           | &ge; 5.5                  | &ge; 5.5                  |
| PostgreSQL      | &ge; 10                   | &ge; 10                   |
| Amazon Redshift | Supported                 | Supported                 |
| Impala          | &ge; 2.5 (CDH &ge; 5.7)   | &ge; 2.5 (CDH &ge; 5.7)   |
| Presto          | Supported                 | Supported                 |
| Spark           | &ge; 2.0                  | Not Supported             |

## Making a Connection in Java

VerdictDB offers the standard JDBC interface. If the `verdict` keyword appears after `jdbc:`, Java uses VerdictDB
to connect to a backend database. In general, they have the following pattern:
```
Connection vc = DriverManager.getConnection("connection_string", "user", "password");
```

1. MySQL:       `"jdbc:verdict:mysql://localhost:8080/test"`
1. PostgreSQL:  `"jdbc:verdict:postgresql://localhost:5432/database"`
1. Redshift:    `"jdbc:verdict:redshift://host:5439/dev"`
1. Impala:      `"jdbc:verdict:impala://localhost:21050/default"`
1. Presto:      `"jdbc:verdict:presto://localhost:8080/default"`

For more details on configuration options, see [this page](/reference/properties/).
