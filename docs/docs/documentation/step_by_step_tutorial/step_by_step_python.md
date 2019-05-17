#Step-by-step Python Tutorial
We will install VerdictDB, create a connection, and issue a simple query to VerdictDB. In this Quickstart Guide, we will use MySQL for VerdictDB's backend database. See [Connecting to Databases](/reference/connection/) for the examples of connecting to other databases.

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

##Create Table and Insert Data
We will first generate small data to play with. Suppose MySQL is set up as described [here](/tutorial/setup/mysql/).
```
mysql_conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='',
    autocommit=True
)
cur = mysql_conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS myschema')
cur.execute('CREATE SCHEMA myschema')
cur.execute(
    'CREATE TABLE myschema.sales (' +
    '   product varchar(100),' +
    '   price   double)'
)
# insert 1000 rows
product_list = ['milk', 'egg', 'juice']
random.seed(0)
for i in range(1000):
    rand_idx = random.randint(0, 2)
    product = product_list[rand_idx]
    price = (rand_idx + 2) * 10 + random.randint(0, 10)
    cur.execute(
        'INSERT INTO myschema.sales (product, price)' +
        '   VALUES ("{:s}", {:f})'.format(product, price)
    )
cur.close()
```

##Create Sample
Create a special table called a "scramble", which is the replica of `sales` with extra information VerdictDB uses for speeding up query processing.
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
Run a regular query to the original table. In `PyVerdict`, The query result is stored in a pandas DataFrame. The values may vary.
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
Internally, VerdictDB rewrites the above query to use the scramble. It is equivalent to explicitly specifying the scramble in the `FROM` clause of the above query.

##Complete Example Script
```
import random
import pymysql
import pyverdict

mysql_conn = pymysql.connect(
    host='localhost',
    port=3306,
    user='root',
    passwd='',
    autocommit=True
)
cur = mysql_conn.cursor()
cur.execute('DROP SCHEMA IF EXISTS myschema')
cur.execute('CREATE SCHEMA myschema')
cur.execute(
    'CREATE TABLE myschema.sales (' +
    '   product varchar(100),' +
    '   price   double)'
)

# insert 1000 rows
product_list = ['milk', 'egg', 'juice']
random.seed(0)
for i in range(1000):
    rand_idx = random.randint(0, 2)
    product = product_list[rand_idx]
    price = (rand_idx + 2) * 10 + random.randint(0, 10)
    cur.execute(
        'INSERT INTO myschema.sales (product, price)' +
        '   VALUES ("{:s}", {:f})'.format(product, price)
    )

cur.close()

# create connection
verdict_conn = pyverdict.mysql(
    host='localhost',
    user='root',
    password='',
    port=3306
)

# create scramble table
verdict_conn.sql('CREATE SCRAMBLE myschema.sales_scrambled from myschema.sales')

# run query
df = verdict_conn.sql(
    "SELECT product, AVG(price) " +
    "FROM myschema.sales " +
    "GROUP BY product " +
    "ORDER BY product")
```
