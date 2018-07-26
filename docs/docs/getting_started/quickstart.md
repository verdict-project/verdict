# Quickstart Guide

We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use an embedded H2 database for VerdictDB's backend database. See [How to Connect](/connection/) for how to use other backend databases.


## Install

Place the following dependency in your maven pom.xml.
```pom
<!-- https://mvnrepository.com/artifact/org.verdictdb/verdictdb-core -->
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>0.5.0-alpha</version>
</dependency>
```


Also, to run verdictDB on H2 database, we need to include dependency for H2.
```pom
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <version>1.4.197</version>
    <scope>test</scope>
</dependency>
```

## Insert Data


```java
Connection h2 = DriverManager.getConnection("jdbc:h2:mem:verdictdb_example_db");
Statement stmt = h2.createStatement();
stmt.execute(
    "create schema \"myschema\"");
stmt.execute(
    "create table \"myschema\".\"sales\" (" +
    "  product   varchar(100)," +
    "  price     double)");
List<List<Object>> contents = new ArrayList<>();
// generate 1000 rows
for (int i=0;i<1000;i++) {
  contents.add(Arrays.<Object>asList(Character.toString ((char) (i%26+65)), Math.random()*100));
}

stmt.execute("CREATE SCHEMA \"MYSCHEMA\"");
stmt.execute("CREATE TABLE \"MYSCHEMA\".\"sales\"(product varchar(100), price double)");
for (List<Object> row : contents) {
  String product = row.get(0).toString();
  String price = row.get(1).toString();
  stmt.execute(String.format("INSERT INTO \"MYSCHEMA\".\"sales\"(product, price) VALUES('%s', %s)", product, price));
}
```



## Connect through VerdictDB

Place the following lines in your Java file.

(*Please change the URL appropriately*)

```java
Connection verdict = DriverManager.getConnection("jdbc:verdictdb:h2:mem:verdictdb_example_db");
```


## Create a Scrambled Table

```java
Statement verdictStmt = verdict.createStatement();
// Use CREATE SCRAMBLE syntax to create scrambled tables.
verdictStmt.execute("CREATE SCRAMBLE \"MYSCHEMA\".\"sales_scrambled\" FROM \"MYSCHEMA\".\"sales\"");
```


## Run a query

```java
ResultSet rs = verdictStmt.executeQuery("SELECT "+
    "AVG(price) from \"MYSCHEMA\".\"sales_scrambled\"" +
    "where price > SELECT AVG(price) from \"MYSCHEMA\".\"sales_scrambled\"");

// Do something after getting the results.
```


## Complete Example Java File

```java
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FirstVerdictDBExample {


  public static void main(String args[]) throws SQLException {
    Connection h2 = DriverManager.getConnection("jdbc:h2:mem:verdictdb_example_db");
    Statement stmt = h2.createStatement();
    stmt.execute(
        "create schema \"myschema\"");
    stmt.execute(
        "create table \"myschema\".\"sales\" (" +
            "  product   varchar(100)," +
            "  price     double)");
    List<List<Object>> contents = new ArrayList<>();
    // generate 1000 rows
    for (int i=0;i<1000;i++) {
      contents.add(Arrays.<Object>asList(Character.toString ((char) (i%26+65)), Math.random()*100));
    }

    stmt.execute("CREATE SCHEMA \"MYSCHEMA\"");
    stmt.execute("CREATE TABLE \"MYSCHEMA\".\"sales\"(product varchar(100), price double)");
    for (List<Object> row : contents) {
      String product = row.get(0).toString();
      String price = row.get(1).toString();
      stmt.execute(String.format("INSERT INTO \"MYSCHEMA\".\"sales\"(product, price) VALUES('%s', %s)", product, price));
    }

    Connection verdict = DriverManager.getConnection("jdbc:verdictdb:h2:mem:verdictdb_example_db");

    Statement verdictStmt = verdict.createStatement();
    // Use CREATE SCRAMBLE syntax to create scrambled tables.
    verdictStmt.execute("CREATE SCRAMBLE \"MYSCHEMA\".\"sales_scrambled\" FROM \"MYSCHEMA\".\"sales\"");

    ResultSet rs = verdictStmt.executeQuery("SELECT "+
        "AVG(price) from \"MYSCHEMA\".\"sales_scrambled\"" +
        "where price > SELECT AVG(price) from \"MYSCHEMA\".\"sales_scrambled\"");

    // Do something after getting the results.
  }
}
```