#Quickstart Java Guide

We will install VerdictDB, create sample, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.

##Installation

###Java
Create an [empty Maven project](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) and
place the verdictdb dependency in the `<dependencies>` of your pom.xml.

###Python

```
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>{{verdictdb.version}}</version>
</dependency>

<!-- To use MySQL, add the following entry as well: -->
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.46</version>
</dependency>
```

##Create Sample
Assume a table `myschema.sales` already exists. Create a JDBC connection to VerdictDB. Create a special table called a "scramble", which is the replica of `schema.sales` with extra information VerdictDB uses for speeding up query processing.
```
Connection verdict =
    DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "");
Statement vstmt = verdict.createStatement();
vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales");
```

##Run Queries
Run a regular query to the original table.
```
ResultSet rs = vstmt.executeQuery(
    "SELECT product, AVG(price) "+
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product");
```
