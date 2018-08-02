# Quickstart Guide

We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use an embedded [H2 database](http://www.h2database.com/) for VerdictDB's backend database. See [How to Connect](/getting_started/connection/) for the examples of connecting to other databases.


## Install

Create a [Maven](https://maven.apache.org/) project and
place the following dependency in your pom.xml.
```pom
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>0.5.0-alpha</version>
</dependency>
```

To use H2, add the following entry as well:
```pom
<dependency>
    <groupId>com.h2database</groupId>
    <artifactId>h2</artifactId>
    <version>1.4.197</version>
</dependency>
```


## Insert Data

*(Yongjoo: double-check the code here)*

We will first generate small data to play with.

```java
Connection h2 = DriverManager.getConnection("jdbc:h2:mem:verdictdb_example_db");
Statement stmt = h2.createStatement();
stmt.execute("create schema \"myschema\"");
stmt.execute("create table \"myschema\".\"sales\" (" +
             "  product   varchar(100)," +
             "  price     double)");

// insert 1000 rows
List<String> productList = Arrays.asList("milk", "egg", "juice");
for (int i = 0; i < 1000; i++) {
  int randInt = ThreadLocalRandom.current().nextInt(0, 3)
  String product = productList.get(randInt);
  double price = (randInt+2) * 10 + ThreadLocalRandom.current().nextInt(0, 10);
  stmt.execute(String.format(
      "INSERT INTO \"myschema\".\"sales\" (product, price) VALUES('%s', %.0f)",
      product, price));
}
```



## Test VerdictDB

*(Yongjoo: double-check the code here)*

Create a JDBC connection to VerdictDB.

```java
Connection verdict = DriverManager.getConnection("jdbc:verdictdb:h2:mem:verdictdb_example_db");
Statement vstmt = verdict.createStatement();
```

Create a special table called a "scramble", which is the replica of the original table with extra information VerdictDB uses for speeding up query processing.

```java
vstmt.execute("create scramble \"myschema\".\"sales_scrambled\" from \"myschema\".\"sales\"");
```

Run just a regular query to the original table.

```java
ResultSet rs = verdictStmt.executeQuery(
    "select product, avg(price) "+
    "from \"myschema\".\"sales\" " +
    "group by product " +
    "order by product");
```

Internally, VerdictDB rewrites the above query to use the scramble. It is equivalent to explicitly specifying the scramble in the from clause of the above query.


## Complete Example Java File

*(Yongjoo: update this according to the above code)*

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