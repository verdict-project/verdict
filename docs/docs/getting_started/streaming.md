# Stream Querying

Stream querying is a feature of VerdictDB that allow users to retrieve results from the query while the query is still processing.

VerdictDB uses scrambles to process the query. In scrambles, original tables are partitioned into many blocks. When processing query, VerdictDB estimates the query, starting from one block and gradually increasing the blocks. The process will finish until covering all blocks(In that case, the results returned by VerdictDB are exactly the same with directly querying from database). Every time the estimation of blocks is done, it will return the result to users. Users can decide whether to have fast but less accurate results or take time to obtain more accurate results.

There are two approaches to VerdictDB's stream querying:

1. VerdicDB JDBC Driver's ```VerdictStatement.sql()``` method
1. VerdictContext's ```streamsql()``` method

We describe them in more detail below.

## Use Stream Querying in VerdictStatement
VerdictStatement implements ```java.sql.Statement``` data type. You can do stream querying by using VerdictStatement.

### Syntax
```
STREAM [SELECT QUERY]
```
Note:

1. [SELECT QUERY] is the SQL query you want to process.
1. Use VerdictStatement.sql() to process the stream querying. VerdictStatement.executeQuery() does not support that syntax.


### Example

First, get a VerdictStatement instance.
```java
Connection verdict = DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "rootpassword");
VerdictStatement vstmt = (VerdictStatement)verdict.createStatement();
```
Assume we want to find the average ```l_extendedprice``` in ```lineitem``` table.
```java
 ResultSet vrs = vstmt.sql("STREAM SELECT AVG(l_extendedprice) from lineitem");
```
Then we can get the approximate results from ```vrs``` in JDBC way.
```java
while (vrs.next()) {
    System.out.println(vrs.getDouble(1));
}
```

!!! note "Note: The behavior of Stream Querying"
    Suppose the scramble of ```lineitem``` has 10 blocks from block0 to block9. VerdictDB will first obtain the result from block0, then block0+block1, then block0+block1+block2 and so on, until it covers all blocks. Once the estimation of block0 is done, it will return the result to ```vrs``` and  start to process block0+block1. So, user will eventually get 10 results from this query in the order of increasing number of covering blocks.
    When user call next() and VerdictDB is still computing for the next result, user will get blocked until the next result is ready.


## Use Stream Querying in VerdictContext
We can issue the query to ```streamsql()``` method to do stream query.

### Example

```java
String connectionString = "jdbc:mysql://localhost?user=root&password=rootpassword";
VerdictContext verdict = VerdictContext.fromJdbcConnectionString(connectionString);
VerdictResultStream vrs = verdict.streamsql("SELECT AVG(l_extendedprice) from lineitem");
```
Note:

1. The input SQL query of ```streamsql()``` does not have the prefix ```STREAM```.
1. ```VerdictResultStream``` type extends ```Iterable<VerdictSingleResult>```, ```Iterator<VerdictSingleResult>```

Retrieve the results of stream querying.
```
while (vrs.hasNext()){
    VerdictSingleResult verdictSingleResult = vrs.next();
    System.out.println(verdictSingleResult.getDouble(1));
}
```

Additionally, you can use the ```verdictSingleResult.print()``` method to print the contents of ```VerdictSingleResult``` look like a table with rows and columns with borders.
For more information about this method, you can check the Java utility class [DBTablePrinter](https://github.com/htorun/dbtableprinter).