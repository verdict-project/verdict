# Same SQL, Same DB, 100x-200x Faster Analytics

Verdict brings you Interactive-speed, resource-efficient data analytics. Its main features are:

1. **200x faster by sacrificing only 1% accuracy**
   Verdict can give you 99% accurate answers for your big data queries in a fraction of the time needed for calculating exact answers. If your data is too big to analyze in a couple of seconds, you will like Verdict.
2. **No change to your database**
   Verdict is a middleware standing between your application and your database. You can just issue the same queries as before and get approximate answers right away. Of course, Verdict handles exact query processing too.
3. **Runs on (almost) any database**
   Verdict can run on any database that supports standard SQL. We already have drivers for Hive, Impala, and MySQL. We’ll soon add drivers for some other popular databases.
4. **Ease of use**
   Verdict is a client-side library: no servers, no port configurations, no extra user authentication, etc. You can simply make a JDBC connection to Verdict; then, Verdict automatically reads data from your database. Verdict is also shipped with a command-line interface.

Find more about Verdict at our website: [VerdictDB.org](http://verdictdb.org).


# How to Use Verdict? Simple!

Let Verdict do some preparation:

```sql
create sample of big_data_table;
```

Now you just issue standard SQL queries and enjoy 100x faster data analytics!

```sql
select city, count(*)
from big_data_table
where arbitrary_attr like '%what i want%'
group by city
order by count(*)
limit 10;
```

Of course, you can issue as many queries as you want after a single preparation for the table you choose; the queries can include arbitrary filtering predicates, joins, etc. See [this page](http://verdictdb.org/documentation/quick_start/) for the examples of different databases.


# Download and Install

You only need to download a couple of jar files to get started. Verdict does not require "sudo" access or any complicated setup process. Go to [this download page](http://verdictdb.org/download/) to find out the files relevant to your data analytics platforms. We already provide pre-built jar files for Cloudera distributions, MapR distributions, and official Apache Spark. You can also build from the source code using the standard build tool, Apache Maven.


# How to Connect?

There are several options, such as (1) Verdict's interactive shell and (2) JDBC driver. For Spark, you simply include Verdict's spark library and issue queries to Verdict's SQL context. Verdict works well with popular front-end tools, such as Apache Zeppelin, Hue, Jupyter notebooks, and so on.


# How are Such Large Speedups Possible?

Verdict speeds up aggregate queries, for which a tiny fraction of the entire data can be used instead for producing highly accurate answers. There are many theories and optimizations as well we developed and implemented inside Verdict for high accuracy and great efficiency. Visit our [research page](http://verdictdb.org/documentation/research/) and see innovations we make.


# Star our Github repo, Use Verdict for free, and Share your story :-)

We maintain Verdict for free under the Apache License so that anyone can benefit from our contributions. If you like our project, please star our Github repository (https://github.com/mozafari/verdict) and send feedback to verdict-user@umich.edu.

