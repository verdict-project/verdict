# Connecting to Databases (Python)

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

## Making a Connection in PyVerdict

`pyverdict` offers simple APIs to make connections to backend databases. A `pyverdict.VerdictContext` object is returned after making a connection. Unique methods are offered for different types of databases.

1. MySQL:       `pyverdict.mysql(host, user, password=None, port=3306)`
1. PostgreSQL:  `pyverdict.postgres(host, port, dbname, user, password=None)`
1. Redshift:    `pyverdict.redshift(host, port, dbname='', user=None, password=None)`
1. Impala:      `pyverdict.impala(host, port, schema=None, user=None, password=None)`
1. Presto:      `pyverdict.presto(host, catalog, user, password=None, port=8081)`
