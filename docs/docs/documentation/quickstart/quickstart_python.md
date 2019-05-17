#Quickstart Python Giude

We will install VerdictDB, create sample, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.

##Installation
`pyverdict` is distributed with [PyPI](https://pypi.org/project/pyverdict/). No installation of VerdictDB is required. To insert data into MySQL in Python without `pyverdict`, we could use [pymysql](https://pymysql.readthedocs.io/en/latest/).

!!! warn "Note: Prerequisites"
    `pyverdict` requires [miniconda](https://conda.io/docs/user-guide/install/index.html) for Python 3.7,
    which can be installed for local users (i.e., without sudo access).


```
pip install pyverdict
# use the following line for upgrading:
# pip install pyverdict --upgrade

# install pymysql to use MySQL
pip install pymysql
```

##Create Sample
Assume a table `myschema.sales` already exists. Create a special table called a "scramble", which is the replica of `myschema.sales` with extra information VerdictDB uses for speeding up query processing.
```
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3306
)
verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')
```

##Run Queries
Run a regular query to the original table. The query result is stored in a pandas DataFrame. The values may vary.
```
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product")
# df
#   product                 a2
# 0     egg  34.82142857142857
# 1   juice  44.96363636363636
# 2    milk  24.97005988023952
```
