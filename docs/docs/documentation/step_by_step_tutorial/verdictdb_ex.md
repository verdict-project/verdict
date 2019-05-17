# Example VerdictDB Application with TPC-H

## VerdictDB on MySQL

On this page, we will demonstrate an example of a Java/Python application with a VerdictDB library that creates a scrambled table for your database, then executes a query that enable VerdictDB to utilize the scrambled table.

Here we assume:

1. Your MySQL database is running
1. TPC-H data has been loaded into your database following the instructions in the previous [Setup TPC-H Data](/tutorial/tpch/) page.


### Getting VerdictDB Example Application

Before we start, you need to get our example Java/Python application that we will be referring to throughout this entire tutorial. To do so, you need `git` and run the following command in your working directory:

```bash
$ git clone git@github.com:verdictdb/verdictdb-tutorial.git
```

This will clone the current VerdictDB tutorial application into `your_working_directory/verdictdb-tutorial`.

Move into the directory with the example we will use.

```bash tab='Java'
cd verdictdb_on_mysql
```

```bash tab='Python'
cd pyverdict_on_mysql
```

### Compiling the Application

The tutorial java application has been set up with Apache Maven and you should be able to build a runnable jar file under `your_working_directory/verdictdb-tutorial/target`.

For the python application, install the required packages with pip.


```bash tab='Java'
mvn package
```

```bash tab='Python'
pip install -r requirements.txt
```


### Running the Application

We included a shell script `run.sh` that you can invoke to execute the example Java application.
The script takes 4 arguments: `hostname`, `port`, `name of database/schema`, and `command`.

### Creating a Scrambled Table

In this tutorial, we are going to create a scrambled table on `lineitem` table. Assume you are running the MySQL database following [this page](/tutorial/setup/mysql/):

```bash tab='Java'
./run.sh localhost 3306 tpch1g create
```

```bash tab='Python'
python tutorial.py create
```

It prints something like this:

```
Creating a scrambled table for lineitem...
Scrambled table for lineitem has been created.
Time Taken = 67 s
```

This command executes the following SQL statement via VerdictDB:

```SQL
CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem
```

Snippet of the corresponding source code (simplified for brevity):

```java tab='Java'
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

```python tab='Python'
...
print('Creating a scrambled table for lineitem...')
start = time.time()
verdict_conn.sql('CREATE SCRAMBLE tpch1g.lineitem_scramble FROM tpch1g.lineitem')
duration = time.time() - start
verdict_conn.close()
print('Scrambled table for lineitem has been created.')
print('Time Taken = {:f} s'.format(duration))
...
```

It's just like running SQL statement via any JDBC connection.

After the scrambled table being created, VerdictDB will automatically utilize this scrambled table for any query that involves the `lineitem` table.

### Running a Sample Query

The example application can execute one very simple aggregation query with and without VerdictDB and compare their performance:

```SQL
SELECT avg(l_extendedprice) FROM tpch1g.lineitem_scramble
```

To run this query with/without VerdictDB, you can simply run the following command:

```bash tab='Java'
./run.sh localhost 3306 tpch1g run
```

```bash tab='Python'
python tutorial.py run
```

The result looks like this:
```
Without VerdictDB: average(l_extendedprice) = 38255.138485
Time Taken = 43 s
With VerdictDB: average(l_extendedprice) = 38219.8871659
Time Taken = 10 s
```

You can see that VerdictDB achieves more than 4x speedup with a 99.9% accurate result even with a relatively small dataset that we use in this tutorial (i.e., 1GB).


## VerdictDB on Apache Spark


We will write a simple example program in Scala, the standard programming language for Apache Spark. To compile our example program, the following tools must be installed.

1. sbt: [sbt installation guide](https://www.scala-sbt.org/1.0/docs/Setup.html)


### Create an empty project

The following command creates a project that prints out "hello".

```bash
$ sbt new sbt/scala-seed.g8

A minimal Scala project.

name [Scala Seed Project]: hello-verdict

Template applied in ./hello-verdict
```

Move into the project directory: `cd hello-verdict`.

Remove the `src/test` directory, which we do not need: `rm -rf src/test`.



### Configure build setting to use Spark and VerdictDB

Add the following line in `build.sbt`, under the existing `import Dependencies._` line. As of the time of writing, the latest version of Apache Spark only supports Scala 2.11.

```scala
scalaVersion := "2.11.1"
```

Also, replace the existing dependency list with

```scala
libraryDependencies ++= Seq(
  scalaTest % Test,
  "org.verdictdb" % "verdictdb-core" % "0.5.8",
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
)
```

This dependency declaration will let the compiler (`sbt` in our case) download relevant libraries automatically.


### Write an example program

Edit `src/main/scala/example/Hello.scala` as follows:

```scala
package example

import org.apache.spark.sql.SparkSession
import org.verdictdb.VerdictContext
import org.verdictdb.connection.SparkConnection
import scala.util.Random

object Hello extends App {
  val spark = SparkSession
    .builder()
    .appName("VerdictDB basic example")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  import spark.implicits._
  val verdict = VerdictContext.fromSparkSession(spark)

  // prepare data
  prepareData(spark, verdict)

  // run a query and print its result
  val rs = verdict.sql("select count(*) from myschema.sales")
  rs.printCsv()

  // simply the following lines will be printed (the actual count value may vary)
  // c2
  // 950.0

  def prepareData(spark: SparkSession, verdict: VerdictContext): Unit = {
    // create a schema and a table
    spark.sql("DROP SCHEMA IF EXISTS myschema CASCADE")
    spark.sql("CREATE SCHEMA IF NOT EXISTS myschema")
    spark.sql("CREATE TABLE IF NOT EXISTS myschema.sales (product string, price double)")

    // insert 1000 rows
    val productList = List("milk", "egg", "juice")
    val rand = new Random()
    var query = "INSERT INTO myschema.sales VALUES"
    for (i <- 0 until 1000) {
      val randInt: Int = rand.nextInt(3)
      val product: String = productList(randInt)
      val price: Double = (randInt+2) * 10 + rand.nextInt(10)
      if (i == 0) {
        query = query + f" ('$product', $price%.0f)"
      } else {
        query = query + f", ('$product', $price%.0f)"
      }
    }
    spark.sql(query)

    verdict.sql("BYPASS DROP TABLE IF EXISTS myschema.sales_scramble")
    verdict.sql("BYPASS DROP SCHEMA IF EXISTS verdictdbtemp CASCADE")
    verdict.sql("BYPASS DROP SCHEMA IF EXISTS verdictdbmeta CASCADE")
    verdict.sql("CREATE SCRAMBLE myschema.sales_scramble FROM myschema.sales BLOCKSIZE 100")
  }
}
```


### Package and Submit

```bash
$ sbt assembly
$ spark-submit target/scala-2.11/Hello-assembly-0.1.0-SNAPSHOT.jar --class example.Hello
```

This example program is available on [this public GitHub repository](https://github.com/verdictdb/verdictdb-tutorial). See the directory `verdictdb_on_spark`.
