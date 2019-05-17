#Quickstart Guide

We will install VerdictDB, create sample, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.

##Installation
### Java
Create an [empty Maven project](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html) and
place the verdictdb dependency in the `<dependencies>` of your pom.xml.

### Python
`pyverdict` is distributed with [PyPI](https://pypi.org/project/pyverdict/). No installation of VerdictDB is required.

To insert data into MySQL in Python without `pyverdict`, we could use [pymysql](https://pymysql.readthedocs.io/en/latest/).

!!! warn "Note: Prerequisites"
    `pyverdict` requires [miniconda](https://conda.io/docs/user-guide/install/index.html) for Python 3.7,
    which can be installed for local users (i.e., without sudo access).

```xml tab='Java'
<dependency>
    <groupId>org.verdictdb</groupId>
    <artifactId>verdictdb-core</artifactId>
    <version>{{verdictdb.version}}</version>
</dependency>

<!-- To use MySQL, add the following entry as well: -->
<dependency>
    <groupId>mysql</groupId>
    <artifactId>mysql-connector-java</artifactId>
    <version>5.1.46</version>
</dependency>
```

```bash tab='Python'
pip install pyverdict
# use the following line for upgrading:
# pip install pyverdict --upgrade

# install pymysql to use MySQL
pip install pymysql
```

##Create Sample
Assume a table `myschema.sales` already exists. Create a connection to VerdictDB. Create a special table called a "scramble", which is the replica of `schema.sales` with extra information VerdictDB uses for speeding up query processing.

```java tab='Java'
Connection verdict =
    DriverManager.getConnection("jdbc:verdict:mysql://localhost", "root", "");
Statement vstmt = verdict.createStatement();
vstmt.execute("CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales");
```

```python tab='Python'
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3306
)
verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')
```


##Run Queries
Run a regular query to the scrambled table to obtain approximated results. In `PyVerdict`, The query result is stored in a pandas DataFrame. The values may vary.

```java tab='Java'
ResultSet rs = vstmt.executeQuery(
    "SELECT product, AVG(price) "+
    "FROM myschema.sales_scrambled " +
    "GROUP BY product " +
    "ORDER BY product");
```

```python tab='Python'
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM myschema.sales_scrambled " +
    "GROUP BY product " +
    "ORDER BY product")
```

<!-- The query result
```
 product          avg(price)
     egg  34.82142857142857
   juice  44.96363636363636
    milk  24.97005988023952
``` -->