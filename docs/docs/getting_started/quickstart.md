# Quickstart

We will install VerdictDB, create a connection, and issue a simple example query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [How to Connect](/connection/) for how to use other backend databases.


## Install

Place the following dependency in your maven pom.xml.

(*please add our maven dependency: https://mvnrepository.com/artifact/org.verdictdb/verdictdb-core/0.5.0-alpha*)

```
pom
```


## Connecting to VerdictDB

Place the following lines in your Java file.

```java
String connectionString = "jdbc:verdictdb:mysql://localhost";
VerdictContext verdict = new VerdictContext(connectionString);
```


## Running an Example Query


