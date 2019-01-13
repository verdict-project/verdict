# Simple Querying

VerdictDB's basic query interface returns a single *(approximate)* result set (i.e., a table) given a query. This is the same interface as what most databases offer; thus, this interface is convenient to use VerdictDB as a drop-in-replacement of other databases.

There are two approaches to VerdictDB's basic query interface:

1. VerdicDB JDBC Driver's regular `executeQuery()` method
2. VerdictContext's `sql()` method

We describe them in more detail below.


## VerdictDB's JDBC Driver

Suppose we aim to query the average price of the items in the `sales` table, i.e., `select avg(price) from sales`. Then, issuing the query with `sales_scramble` table in a traditional way to the VerdictDB's JDBC interface returns an approximate answer.

The below code shows an example.

```java
Connection verdict = DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "rootpassword");
Statement stmt = verdict.createStatement();
ResultSet rs = stmt.executeQuery("select avg(price) from sales_scramble");
```

!!! note "Note: Direct querying to the backend"
    You can always send a query directly to the backend database by preceding a query with the `bypass` keyword, e.g., `bypass select avg(price) from sales`. Then, VerdictDB sends the query to the backend database without any query rewriting or extra processing. This approach can be used for all other queries, such as `create schema ...`, `set key=value`, etc.


## VerdictContext

Issuing a query using `VerdictContext.sql()` method returns a single approximate query result. See the example below.

```java
String connectionString = "jdbc:mysql://localhost?user=root&password=rootpassword";
VerdictContext verdict = VerdictContext.fromJdbcConnectionString(connectionString);
VerdictSingleResult rs = verdict.sql("select avg(price) from sales_scramble");
```

!!! note
    When creating an instance of VerdictContext, the JDBC url must not include the "verdict" keyword.
