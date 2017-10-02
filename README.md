# Verdict: 100x-200x Faster Data Analytics

Verdict brings you Interactive-speed, resource-efficient query processor exploiting approximate query processing. Its main features are:

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


# Download and Install

Downloading a couple of jar files is the end of installing Verdict. Visit [this page](http://verdictdb.org/download/) to find out the files releveant to your data analytics platforms. We already provide pre-built jar files for Cloudera distributions, MapR distributions, and official Apache Spark. You can also build from the source code using the standard build tool: Apache Maven.


# Quick Start

See [this page](http://verdictdb.org/documentation/quick_start/) to see how to generate sample tables and to see Verdict automatically uses those sample tables to speed up your queries.


