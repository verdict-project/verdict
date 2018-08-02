# Quickstart Guide

We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use an embedded H2 database for VerdictDB's backend database. See [How to Connect](/connection/) for how to use other backend databases.


## Install

Place the following dependency in your maven pom.xml.

(*please add our maven dependency: https://mvnrepository.com/artifact/org.verdictdb/verdictdb-core/0.5.0-alpha*)

(*also add the pom for H2*)

```
pom
```

## Insert Data


```java
Connection h2 = DriverManager.getConnection();
h2.createStatement().execute(    
    "create table \"sales\" (" +
    "  product   varchar(100)," +
    "  price     double)");
```



## Connect through VerdictDB

Place the following lines in your Java file.

(*Please change the URL appropriately*)

```java
Connection verdict = DriverManager.getConnection("jdbc:verdictdb:h2://localhost");
```


## Create a Scrambled Table

```java
```


## Run a query

```java
```


## Complete Example Java File

```java
```