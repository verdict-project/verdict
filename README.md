# Verdict: Interactive-speed, resource-efficient query processor

Verdict is an approximate, big data analytics system. Verdict is useful because:

1. **200x faster by sacrificing only 1% accuracy**
   Verdict can give you 99% accurate answers for your big data queries in a
   fraction of the time needed for calculating exact answers. If your data is
   too big to analyze in a couple of seconds, you will like Verdict.
2. **No change to your database**
   Verdict is a middleware standing between your application and your database.
   You can just issue the same queries as before and get approximate answers
   right away. Of course, Verdict handles exact query processing too.
3. **Runs on (almost) any database**
   Verdict can run on any database that supports standard SQL. We already have
   drivers for Hive, Impala, and MySQL. We’ll soon add drivers for some other
   popular databases.
4. **Ease of use**
   Verdict is a client-side library: no servers, no port configurations, no
   extra user authentication, etc. You can simply make a JDBC connection to
   Verdict; then, Verdict automatically reads data from your database. Verdict
   is also shipped with a command-line interface.

Find more about Verdict at our website: [VerdictDB.org](http://verdictdb.org).


# Getting Started

Verdict currently runs on [Apache Spark](https://spark.apache.org/), [Apache (incubating) Impala](https://impala.incubator.apache.org/), and [Apache Hive](https://hive.apache.org/). We are adding drivers for other database systems.

Using Verdict is easy. Following this guide, you can finish setup in five minutes if you have any of those supported systems ready.

## Building Verdict

Download and unzip this [zip file](). Then, simply type `mvn package` in the unzipped directory. The command will download all the dependencies and compile Verdict's code. The command will create three `jar` files in the `target` directory. Then, building Verdict is done!

### More details

Verdict is tested on Oracle JDK 1.7 or above, but it should work with open JDK, too. `mvn` is the command for the [Apache Maven](https://maven.apache.org/) package manager. If you do not have the Maven, you will have to install it. The official page for the Maven installiation is [this](https://maven.apache.org/install.html).


## Using Verdict

The steps for starting Verdict is slightly different depending on the database system it works on. Once connected, however, Verdict accepts the same SQL statements.


### On Apache Spark

We show how to use Verdict in `spark-shell` and `pyspark`. Using Verdict in an Spark application written either in Scala or Python is the same.

#### Verdict-on-Spark

You can start `spark-shell` with Verdict as follows.

```bash
$ spark-shell --jars target/verdict-core-0.3.0-jar-with-dependencies.jar
```

After spark-shell starts, import and use Verdict as follows.

```scala
import edu.umich.verdict.VerdictSparkHiveContext
val vc = new VerdictSparkHiveContext(sc)   // sc: SparkContext instance
vc.sql("show databases").show(false)       // Simply displays the databases (or schemas)
// Creates samples for the table. This step needs to be done only once for the table.
vc.sql("create sample of database_name.table_name").show(false)
// Now Verdict automatically uses available samples for speeding up this query.
vc.sql("select count(*) from database_name.table_name").show()
```

The return value of `VerdictSparkHiveContext#sql()` is a Spark's DataFrame class; thus, any methods that work on Spark's DataFrame work on Verdict's answer seamlessly.


#### Verdict-on-PySpark

You can start `pyspark` shell with Verdict as follows.

```bash
$ export PYTHONPATH=$(pwd)/target:$PYTHONPATH
$ pyspark --driver-class-path target/verdict-core-0.3.0-jar-with-dependencies.jar
```

**Limitation**: Note that, in order for the `--driver-class-path` option to work, the jar file (i.e., `target/verdict-core-0.3.0-jar-with-dependencies.jar`) must be present in the Spark's driver node. Verdict will support `--jars` option shortly (this is due to the bug in pyspark).

After pyspark shell starts, import and use Verdict as follows.

```python
from pyverdict import VerdictHiveContext
vc = VerdictHiveContext(sc)        # sc: SparkContext instance
vc.sql("show databases").show()    # Simply displays the databases (or schemas)
# Creates samples for the table. This step needs to be done only once for the table.
vc.sql("create sample of database_name.table_name").show()
# Now Verdict automatically uses available samples for speeding up this query.
vc.sql("select count(*) from database_name.table_name").show()
```

The return value of `VerdictHiveContext#sql()` is a pyspark's DataFrame class; thus, any methods that work on pyspark's DataFrame work on Verdict's answer seamlessly.


### On Apache Impala or Apache Hive

We will use our command line interface (which is called `veeline`) for connecting to those databases. You can programmatically connect to Verdict using the standard JDBC interface, too. Please see [our website](http://verdictdb.org) for the JDBC instruction.

#### Verdict-on-Impala

Type the following command in terminal to launch `veeline` that connects to Impala.

```bash
$ veeline/bin/veeline -h "impala://hostname:port/schema;key1=value1;key2=value2;..." -u username -p password
```

Note that parameters are delimited using semicolons (`;`). The connection string is quoted since the semicolons have special meaning in bash. The user name and password can be passed in the connetion string as parameters, too.

Verdict supports the Kerberos connection. For this, add `principal=user/host@domain` as one of those key-values pairs.

After `veeline` launches, you can issue regular SQL queries as follows.

```bash
verdict:impala> show databases;
verdict:impala> create sample of database_name.table_name;
verdict:impala> select count(*) from database_name.table_name;
```

#### Verdict-on-Hive

Type the following command in terminal to launch `veeline` that connects to Hive.

```bash
$ veeline/bin/veeline -h "hive2://hostname:port/schema;key1=value1;key2=value2;..." -u username -p password
```

Note that parameters are delimited using semicolons (`;`). The connection string is quoted since the semicolons have special meaning in bash. The user name and password can be passed in the connetion string as parameters, too.

Verdict supports the Kerberos connection. For this, add `principal=user/host@domain` as one of those key-values pairs.

After `veeline` launches, you can issue regular SQL queries as follows.

```bash
verdict:Apache Hive> show databases;
verdict:Apache Hive> create sample of database_name.table_name;
verdict:Apache Hive> select count(*) from database_name.table_name;
```

#### Notes on using `veeline`

`veeline` makes a JDBC connection to the database systems that Verdict work on top of (e.g., Impala or Hive). For this, it uses the JDBC drivers stored in the `lib` folder. Our code ships by default with the Cloudera's Impala and Hive JDBC drivers (jar files). However, if these drivers are not compatible with your environment, you can put the compatible JDBC drivers in the `lib` folder after deleting existing ones.


## What's Next

See what types of queries are supported by Verdict in [our website](http://verdictdb.org), and enjoy the speedup provided Verdict for those queries.

If you have use cases that are not supported by Verdict, please contact us at `verdict-user@umich.edu`, or create an issue in our Github repository. We will answer your questions or requests shortly (at most in a few days).
