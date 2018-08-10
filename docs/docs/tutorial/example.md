# An Example VerdictDB Application

On this page, we will demonstrate an example of a Java application with a VerdictDB library that creates a scrambled table for your database, then executes a query that enable VerdictDB to utilize the scrambled table. 

Here we assume 1) your MySQL database is running and 
2) TPC-H data has been loaded into your database following the instructions in the previous **Setup TPC-H Data** page.

## Getting VerdictDB Example Application

Before we start, you need to get our example Java application that we will be referring to throughout this entire tutorial. To do so, you need `git` and run the following command in your working directory:

```bash
$ git clone git@github.com:verdictdb/verdictdb-tutorial.git
```

This will clone the current VerdictDB tutorial application into `your_working_directory/verdictdb-tutorial`.

## Compiling the Application

The tutorial application has been set up with `maven` and you should be able to build a runnable jar file by running the following command:

```bash
$ mvn package
```

This will create a runnable jar file under `your_working_directory/verdictdb-tutorial/target`.

## Running the Application

We included a shell script `run.sh` in the example application that you can invoke to execute the example application.
The script takes 4 arguments: `hostname`, `port`, `name of database/schema`, and `command`.

## Creating a Scrambled Table

In this tutorial, we are going to create a scrambled table on `lineitem` table. 
With the example application, you can create one by simply running the following command (assuming you are running the MySQL database following our **Setup MySQL** page):

```bash
$ ./run.sh localhost 3306 tpch1g create
Creating a scrambled table for lineitem...
Scrambled table for lineitem has been created.
Time Taken = 67 s
```

This command executes the following SQL statement via VerdictDB:

```SQL
CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem
```

A corresponding Java source in the example application is following (simplified for brevity):

```java
  ...
  Statement stmt = verdictConn.createStatement();
  String createQuery =
      String.format(
          "CREATE SCRAMBLE %s.lineitem_scramble " + "FROM %s.lineitem",
          database, database);
  System.out.println("Creating a scrambled table for lineitem...");
  stmt.execute(createQuery);
  stmt.close();
  System.out.println("Scrambled table for lineitem has been created.");
  ...
```

As it is clear from the above snippet, it is just like running SQL statement via any JDBC connection.

After the scrambled table is created, VerdictDB will automatically utilize this scrambled table for any query that involves the `lineitem` table.

## Running a Sample Query

The example application can execute one very simple aggregation query with and without VerdictDB, which lets you to see the performance difference between the two cases.

The actual query is as follows:

```SQL
SELECT avg(l_extendedprice) FROM tpch1g.lineitem
```

To run this query with/without VerdictDB, you can simply run the following command:

```bash
$ ./run.sh localhost 3306 tpch1g run
Without VerdictDB: average(l_extendedprice) = 38255.138485
Time Taken = 43 s
With VerdictDB: average(l_extendedprice) = 38219.8871659
Time Taken = 10 s
```

You can see that VerdictDB achieves more than 4x speedup with a 99.9% accurate result even with a relatively small dataset that we use in this tutorial (i.e., 1GB).








