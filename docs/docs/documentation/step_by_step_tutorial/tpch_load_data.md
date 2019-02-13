# TPC-H Data Setup

This is a step-by-step guide for setting up TPC-H data in different databases. This guide will use 1GB data. This guide assumes you have basic knowledge about issuing commands in a terminal application.


## Download Data

Go to your work directory (say `/home/username/workspace`) and download the data:

```bash
$ cd /home/username/workspace
$ curl http://dbgroup-internal.eecs.umich.edu/projects/verdictdb/tpch1g.zip -o tpch1g.zip
```

Unzip the downloaded file:

```bash
$ unzip tpch1g.zip
```

It will create a new directory named `tpch1g` under your work directory. The directory contains 8 sub-directories for each of 8 tables.


## MySQL

### Create tables

Connect to your MySQL database. Make sure you have already added MySQL to your PATH.

```bash
$ mysql --local-infile -h 127.0.0.1 -uroot
```

Create a schema for test. (In mysql shell)

```bash
> create database tpch1g;
> use tpch1g;
```

Create empty tables; simply copy and paste the following table definition statements into the MySQL shell. We will import the data later into these tables.

```sql
-- nation
CREATE TABLE IF NOT EXISTS tpch1g.nation (
  `n_nationkey`  INT,
  `n_name`       CHAR(25),
  `n_regionkey`  INT,
  `n_comment`    VARCHAR(152),
  `n_dummy`      VARCHAR(10),
  PRIMARY KEY (`n_nationkey`));

-- region
CREATE TABLE IF NOT EXISTS tpch1g.region (
  `r_regionkey`  INT,
  `r_name`       CHAR(25),
  `r_comment`    VARCHAR(152),
  `r_dummy`      VARCHAR(10),
  PRIMARY KEY (`r_regionkey`));

-- supplier
CREATE TABLE IF NOT EXISTS tpch1g.supplier (
  `s_suppkey`     INT,
  `s_name`        CHAR(25),
  `s_address`     VARCHAR(40),
  `s_nationkey`   INT,
  `s_phone`       CHAR(15),
  `s_acctbal`     DECIMAL(15,2),
  `s_comment`     VARCHAR(101),
  `s_dummy` varchar(10),
  PRIMARY KEY (`s_suppkey`));

-- customer
CREATE TABLE IF NOT EXISTS tpch1g.customer (
  `c_custkey`     INT,
  `c_name`        VARCHAR(25),
  `c_address`     VARCHAR(40),
  `c_nationkey`   INT,
  `c_phone`       CHAR(15),
  `c_acctbal`     DECIMAL(15,2),
  `c_mktsegment`  CHAR(10),
  `c_comment`     VARCHAR(117),
  `c_dummy`       VARCHAR(10),
  PRIMARY KEY (`c_custkey`));

-- part
CREATE TABLE IF NOT EXISTS tpch1g.part (
  `p_partkey`     INT,
  `p_name`        VARCHAR(55),
  `p_mfgr`        CHAR(25),
  `p_brand`       CHAR(10),
  `p_type`        VARCHAR(25),
  `p_size`        INT,
  `p_container`   CHAR(10),
  `p_retailprice` DECIMAL(15,2) ,
  `p_comment`     VARCHAR(23) ,
  `p_dummy`       VARCHAR(10),
  PRIMARY KEY (`p_partkey`));

-- partsupp
CREATE TABLE IF NOT EXISTS tpch1g.partsupp (
  `ps_partkey`     INT,
  `ps_suppkey`     INT,
  `ps_availqty`    INT,
  `ps_supplycost`  DECIMAL(15,2),
  `ps_comment`     VARCHAR(199),
  `ps_dummy`       VARCHAR(10),
  PRIMARY KEY (`ps_partkey`));

-- orders
CREATE TABLE IF NOT EXISTS tpch1g.orders (
  `o_orderkey`       INT,
  `o_custkey`        INT,
  `o_orderstatus`    CHAR(1),
  `o_totalprice`     DECIMAL(15,2),
  `o_orderdate`      DATE,
  `o_orderpriority`  CHAR(15),
  `o_clerk`          CHAR(15),
  `o_shippriority`   INT,
  `o_comment`        VARCHAR(79),
  `o_dummy`          VARCHAR(10),
  PRIMARY KEY (`o_orderkey`));

-- lineitem
CREATE TABLE IF NOT EXISTS tpch1g.lineitem (
  `l_orderkey`    INT,
  `l_partkey`     INT,
  `l_suppkey`     INT,
  `l_linenumber`  INT,
  `l_quantity`    DECIMAL(15,2),
  `l_extendedprice`  DECIMAL(15,2),
  `l_discount`    DECIMAL(15,2),
  `l_tax`         DECIMAL(15,2),
  `l_returnflag`  CHAR(1),
  `l_linestatus`  CHAR(1),
  `l_shipdate`    DATE,
  `l_commitdate`  DATE,
  `l_receiptdate` DATE,
  `l_shipinstruct` CHAR(25),
  `l_shipmode`    CHAR(10),
  `l_comment`     VARCHAR(44),
  `l_dummy`       VARCHAR(10));
```

### Import Data

Suppose your work directory is `/home/username/workspace` and the tpch1g data is stored in `/home/username/workspace/tpch1g`. Then, issue the following commands in the MySQL shell to load the data.

```sql
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/region/region.tbl'     INTO TABLE region     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/nation/nation.tbl'     INTO TABLE nation     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/customer/customer.tbl' INTO TABLE customer   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/supplier/supplier.tbl' INTO TABLE supplier   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/part/part.tbl'         INTO TABLE part       FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/partsupp/partsupp.tbl' INTO TABLE partsupp   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/orders/orders.tbl'     INTO TABLE orders     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/home/username/workspace/tpch1g/lineitem/lineitem.tbl' INTO TABLE lineitem   FIELDS TERMINATED BY '|';
```

## PostgreSQL

### Create tables

Connect to your Postgresql database.

```bash
$ psql
```

Create a schema for testing.

```
postgres=# create schema tpch1g;
postgres=# set search_path to tpch1g;
```

Create empty tables; simply copy and paste the following table definition statements into the PostgreSQL prompt. We will import the data later into these tables.

```sql
-- nation
CREATE TABLE IF NOT EXISTS "nation" (
  "n_nationkey"  INT,
  "n_name"       CHAR(25),
  "n_regionkey"  INT,
  "n_comment"    VARCHAR(152),
  "n_dummy"      VARCHAR(10),
  PRIMARY KEY ("n_nationkey"));

-- region
CREATE TABLE IF NOT EXISTS "region" (
  "r_regionkey"  INT,
  "r_name"       CHAR(25),
  "r_comment"    VARCHAR(152),
  "r_dummy"      VARCHAR(10),
  PRIMARY KEY ("r_regionkey"));

-- supplier
CREATE TABLE IF NOT EXISTS "supplier" (
  "s_suppkey"     INT,
  "s_name"        CHAR(25),
  "s_address"     VARCHAR(40),
  "s_nationkey"   INT,
  "s_phone"       CHAR(15),
  "s_acctbal"     DECIMAL(15,2),
  "s_comment"     VARCHAR(101),
  "s_dummy"       VARCHAR(10),
  PRIMARY KEY ("s_suppkey"));

-- customer
CREATE TABLE IF NOT EXISTS "customer" (
  "c_custkey"     INT,
  "c_name"        VARCHAR(25),
  "c_address"     VARCHAR(40),
  "c_nationkey"   INT,
  "c_phone"       CHAR(15),
  "c_acctbal"     DECIMAL(15,2),
  "c_mktsegment"  CHAR(10),
  "c_comment"     VARCHAR(117),
  "c_dummy"       VARCHAR(10),
  PRIMARY KEY ("c_custkey"));

-- part
CREATE TABLE IF NOT EXISTS "part" (
  "p_partkey"     INT,
  "p_name"        VARCHAR(55),
  "p_mfgr"        CHAR(25),
  "p_brand"       CHAR(10),
  "p_type"        VARCHAR(25),
  "p_size"        INT,
  "p_container"   CHAR(10),
  "p_retailprice" DECIMAL(15,2) ,
  "p_comment"     VARCHAR(23) ,
  "p_dummy"       VARCHAR(10),
  PRIMARY KEY ("p_partkey"));

-- partsupp
CREATE TABLE IF NOT EXISTS "partsupp" (
  "ps_partkey"     INT,
  "ps_suppkey"     INT,
  "ps_availqty"    INT,
  "ps_supplycost"  DECIMAL(15,2),
  "ps_comment"     VARCHAR(199),
  "ps_dummy"       VARCHAR(10),
  PRIMARY KEY ("ps_partkey"));

-- orders
CREATE TABLE IF NOT EXISTS "orders" (
  "o_orderkey"       INT,
  "o_custkey"        INT,
  "o_orderstatus"    CHAR(1),
  "o_totalprice"     DECIMAL(15,2),
  "o_orderdate"      DATE,
  "o_orderpriority"  CHAR(15),
  "o_clerk"          CHAR(15),
  "o_shippriority"   INT,
  "o_comment"        VARCHAR(79),
  "o_dummy"          VARCHAR(10),
  PRIMARY KEY ("o_orderkey"));

-- lineitem
CREATE TABLE IF NOT EXISTS "lineitem"(
  "l_orderkey"          INT,
  "l_partkey"           INT,
  "l_suppkey"           INT,
  "l_linenumber"        INT,
  "l_quantity"          DECIMAL(15,2),
  "l_extendedprice"     DECIMAL(15,2),
  "l_discount"          DECIMAL(15,2),
  "l_tax"               DECIMAL(15,2),
  "l_returnflag"        CHAR(1),
  "l_linestatus"        CHAR(1),
  "l_shipdate"          DATE,
  "l_commitdate"        DATE,
  "l_receiptdate"       DATE,
  "l_shipinstruct"      CHAR(25),
  "l_shipmode"          CHAR(10),
  "l_comment"           VARCHAR(44),
  "l_dummy"             VARCHAR(10));
```

### Import Data

Suppose your work directory is `/home/username/workspace` and the tpch1g data is stored in `/home/username/workspace/tpch1g`. Then, issue the following commands in the PostgreSQL prompt to load the data.

```bash
\copy "region"     from '/home/username/workspace/tpch1g/region/region.tbl'        DELIMITER '|' CSV;
\copy "nation"     from '/home/username/workspace/tpch1g/nation/nation.tbl'        DELIMITER '|' CSV;
\copy "customer"   from '/home/username/workspace/tpch1g/customer/customer.tbl'    DELIMITER '|' CSV;
\copy "supplier"   from '/home/username/workspace/tpch1g/supplier/supplier.tbl'    DELIMITER '|' CSV;
\copy "part"       from '/home/username/workspace/tpch1g/part/part.tbl'            DELIMITER '|' CSV;
\copy "partsupp"   from '/home/username/workspace/tpch1g/partsupp/partsupp.tbl'    DELIMITER '|' CSV;
\copy "orders"     from '/home/username/workspace/tpch1g/orders/orders.tbl'        DELIMITER '|' CSV;
\copy "lineitem"   from '/home/username/workspace/tpch1g/lineitem/lineitem.tbl'    DELIMITER '|' CSV;
```

## Apache Spark

### Put data to HDFS

Use following commands to put data into HDFS. Suppose the tpch1g data is stored in `/home/username/workspace/tpch1g` and you hope to put your data in `/tmp/tpch1g` in HDFS.
```bash
$ hdfs dfs -mkdir -p /tmp/tpch1g/region
$ hdfs dfs -mkdir -p /tmp/tpch1g/nation
$ hdfs dfs -mkdir -p /tmp/tpch1g/customer
$ hdfs dfs -mkdir -p /tmp/tpch1g/supplier
$ hdfs dfs -mkdir -p /tmp/tpch1g/part
$ hdfs dfs -mkdir -p /tmp/tpch1g/partsupp
$ hdfs dfs -mkdir -p /tmp/tpch1g/orders
$ hdfs dfs -mkdir -p /tmp/tpch1g/lineitem
$ hdfs dfs -put /home/username/workspace/tpch1g/region/region.tbl       /tmp/tpch1g/region
$ hdfs dfs -put /home/username/workspace/tpch1g/nation/nation.tbl       /tmp/tpch1g/nation
$ hdfs dfs -put /home/username/workspace/tpch1g/customer/customer.tbl   /tmp/tpch1g/customer
$ hdfs dfs -put /home/username/workspace/tpch1g/supplier/supplier.tbl   /tmp/tpch1g/supplier
$ hdfs dfs -put /home/username/workspace/tpch1g/part/part.tbl           /tmp/tpch1g/part
$ hdfs dfs -put /home/username/workspace/tpch1g/partsupp/partsupp.tbl   /tmp/tpch1g/partsupp
$ hdfs dfs -put /home/username/workspace/tpch1g/orders/orders.tbl       /tmp/tpch1g/orders
$ hdfs dfs -put /home/username/workspace/tpch1g/lineitem/lineitem.tbl   /tmp/tpch1g/lineitem
```

If you encounter write permission problem in the next step when creating tables, you can use command
```bash
$ hdfs dfs -chmod -R 777 /tmp/tpch1g
```
to give full access to your directory.

### Create table and load data

Simply copy and paste following queries to spark to set up TPC-H tables.

```sql
-- nation
CREATE TABLE IF NOT EXISTS nation (
  `n_nationkey`  INT,
  `n_name`       CHAR(25),
  `n_regionkey`  INT,
  `n_comment`    VARCHAR(152),
  `n_dummy`      VARCHAR(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/nation/';

-- region
CREATE TABLE IF NOT EXISTS region (
  `r_regionkey`  INT,
  `r_name`       CHAR(25),
  `r_comment`    VARCHAR(152),
  `r_dummy`      VARCHAR(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/region/';


-- supplier
CREATE TABLE IF NOT EXISTS supplier (
  `s_suppkey`     INT,
  `s_name`        CHAR(25),
  `s_address`     VARCHAR(40),
  `s_nationkey`   INT,
  `s_phone`       CHAR(15),
  `s_acctbal`     DECIMAL(15,2),
  `s_comment`     VARCHAR(101),
  `s_dummy` varchar(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/supplier/';


-- customer
CREATE TABLE IF NOT EXISTS customer (
  `c_custkey`     INT,
  `c_name`        VARCHAR(25),
  `c_address`     VARCHAR(40),
  `c_nationkey`   INT,
  `c_phone`       CHAR(15),
  `c_acctbal`     DECIMAL(15,2),
  `c_mktsegment`  CHAR(10),
  `c_comment`     VARCHAR(117),
  `c_dummy`       VARCHAR(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/customer/';


-- part
CREATE TABLE IF NOT EXISTS part (
  `p_partkey`     INT,
  `p_name`        VARCHAR(55),
  `p_mfgr`        CHAR(25),
  `p_brand`       CHAR(10),
  `p_type`        VARCHAR(25),
  `p_size`        INT,
  `p_container`   CHAR(10),
  `p_retailprice` DECIMAL(15,2) ,
  `p_comment`     VARCHAR(23) ,
  `p_dummy`       VARCHAR(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/part/';

-- partsupp
CREATE TABLE IF NOT EXISTS partsupp (
  `ps_partkey`     INT,
  `ps_suppkey`     INT,
  `ps_availqty`    INT,
  `ps_supplycost`  DECIMAL(15,2),
  `ps_comment`     VARCHAR(199),
  `ps_dummy`       VARCHAR(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/partsupp/';

-- orders
CREATE TABLE IF NOT EXISTS orders (
  `o_orderkey`       INT,
  `o_custkey`        INT,
  `o_orderstatus`    CHAR(1),
  `o_totalprice`     DECIMAL(15,2),
  `o_orderdate`      DATE,
  `o_orderpriority`  CHAR(15),
  `o_clerk`          CHAR(15),
  `o_shippriority`   INT,
  `o_comment`        VARCHAR(79),
  `o_dummy`          VARCHAR(10))
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
   STORED AS TEXTFILE
   LOCATION '/tmp/tpch1g/orders/';

-- lineitem
CREATE TABLE IF NOT EXISTS lineitem (
  `l_orderkey`          INT,
  `l_partkey`           INT,
  `l_suppkey`           INT,
  `l_linenumber`        INT,
  `l_quantity`          DECIMAL(15,2),
  `l_extendedprice`     DECIMAL(15,2),
  `l_discount`          DECIMAL(15,2),
  `l_tax`               DECIMAL(15,2),
  `l_returnflag`        CHAR(1),
  `l_linestatus`        CHAR(1),
  `l_shipdate`          DATE,
  `l_commitdate`        DATE,
  `l_receiptdate`       DATE,
  `l_shipinstruct`      CHAR(25),
  `l_shipmode`          CHAR(10),
  `l_comment`           VARCHAR(44),
  `l_dummy`             VARCHAR(10))
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  STORED AS TEXTFILE
  LOCATION '/tmp/tpch1g/lineitem/';
```


## Redshift

### Create tables

Use SQL query tools like [SQL Workbench/J](http://www.sql-workbench.eu/) to connect with your Redshift Cluster.

First, Create a schema for testing
```sql
create schema "tpch1g";
```
Then Create empty tables.
```sql
-- nation
CREATE TABLE IF NOT EXISTS "tpch1g"."nation" (
  "n_nationkey"  INT,
  "n_name"       CHAR(25),
  "n_regionkey"  INT,
  "n_comment"    VARCHAR(152),
  "n_dummy"      VARCHAR(10),
  PRIMARY KEY ("n_nationkey"));

-- region
CREATE TABLE IF NOT EXISTS "tpch1g"."region" (
  "r_regionkey"  INT,
  "r_name"       CHAR(25),
  "r_comment"    VARCHAR(152),
  "r_dummy"      VARCHAR(10),
  PRIMARY KEY ("r_regionkey"));

-- supplier
CREATE TABLE IF NOT EXISTS "tpch1g"."supplier" (
  "s_suppkey"     INT,
  "s_name"        CHAR(25),
  "s_address"     VARCHAR(40),
  "s_nationkey"   INT,
  "s_phone"       CHAR(15),
  "s_acctbal"     DECIMAL(15,2),
  "s_comment"     VARCHAR(101),
  "s_dummy"       VARCHAR(10),
  PRIMARY KEY ("s_suppkey"));

-- customer
CREATE TABLE IF NOT EXISTS "tpch1g"."customer" (
  "c_custkey"     INT,
  "c_name"        VARCHAR(25),
  "c_address"     VARCHAR(40),
  "c_nationkey"   INT,
  "c_phone"       CHAR(15),
  "c_acctbal"     DECIMAL(15,2),
  "c_mktsegment"  CHAR(10),
  "c_comment"     VARCHAR(117),
  "c_dummy"       VARCHAR(10),
  PRIMARY KEY ("c_custkey"));

-- part
CREATE TABLE IF NOT EXISTS "tpch1g"."part" (
  "p_partkey"     INT,
  "p_name"        VARCHAR(55),
  "p_mfgr"        CHAR(25),
  "p_brand"       CHAR(10),
  "p_type"        VARCHAR(25),
  "p_size"        INT,
  "p_container"   CHAR(10),
  "p_retailprice" DECIMAL(15,2) ,
  "p_comment"     VARCHAR(23) ,
  "p_dummy"       VARCHAR(10),
  PRIMARY KEY ("p_partkey"));

-- partsupp
CREATE TABLE IF NOT EXISTS "tpch1g"."partsupp" (
  "ps_partkey"     INT,
  "ps_suppkey"     INT,
  "ps_availqty"    INT,
  "ps_supplycost"  DECIMAL(15,2),
  "ps_comment"     VARCHAR(199),
  "ps_dummy"       VARCHAR(10),
  PRIMARY KEY ("ps_partkey"));

-- orders
CREATE TABLE IF NOT EXISTS "tpch1g"."orders" (
  "o_orderkey"       INT,
  "o_custkey"        INT,
  "o_orderstatus"    CHAR(1),
  "o_totalprice"     DECIMAL(15,2),
  "o_orderdate"      DATE,
  "o_orderpriority"  CHAR(15),
  "o_clerk"          CHAR(15),
  "o_shippriority"   INT,
  "o_comment"        VARCHAR(79),
  "o_dummy"          VARCHAR(10),
  PRIMARY KEY ("o_orderkey"));

-- lineitem
CREATE TABLE IF NOT EXISTS "tpch1g"."lineitem"(
  "l_orderkey"          INT,
  "l_partkey"           INT,
  "l_suppkey"           INT,
  "l_linenumber"        INT,
  "l_quantity"          DECIMAL(15,2),
  "l_extendedprice"     DECIMAL(15,2),
  "l_discount"          DECIMAL(15,2),
  "l_tax"               DECIMAL(15,2),
  "l_returnflag"        CHAR(1),
  "l_linestatus"        CHAR(1),
  "l_shipdate"          DATE,
  "l_commitdate"        DATE,
  "l_receiptdate"       DATE,
  "l_shipinstruct"      CHAR(25),
  "l_shipmode"          CHAR(10),
  "l_comment"           VARCHAR(44),
  "l_dummy"             VARCHAR(10));
```

### Load Data
For Redshift, we use Java method to help inserting our data into Redshift tables. `schema` is the Redshift schema you create your TPC-H tables
and `table` is the table name of TPCH-table, such as `nation`, `region`. `conn` is the Connection class you get from connecting your Redshift cluster using Redshift JDBC driver.
Suppose your work directory is `/home/username/workspace` and the tpch1g data is stored in `/home/username/workspace/tpch1g`.
```java
static void loadRedshiftData(String schema, String table, Connection conn)
      throws IOException {
    Statement stmt = conn.createStatement();
    String concat = "";
    File file =
        new File(String.format("/home/username/workspace/tpch1g/%s/%s.tbl", table, table));
    ResultSet columnMeta =
        stmt.execute(
            String.format(
                "select data_type, ordinal_position from INFORMATION_SCHEMA.COLUMNS where table_name='%s' and table_schema='%s'",
                table, schema));
    List<Boolean> quotedNeeded = new ArrayList<>();
    for (int i = 0; i < columnMeta.getRowCount(); i++) {
      quotedNeeded.add(true);
    }
    while (columnMeta.next()) {
      String columnType = columnMeta.getString(1);
      int columnIndex = columnMeta.getInt(2);
      if (columnType.equals("integer") || columnType.equals("numeric")) {
        quotedNeeded.set(columnIndex - 1, false);
      }
    }

    String content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = "";
      for (int i = 0; i < values.length - 1; i++) {
        if (quotedNeeded.get(i)) {
          row = row + getQuoted(values[i]) + ",";
        } else {
          row = row + values[i] + ",";
        }
      }
      row = row + "''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      } else concat = concat + "," + "(" + row + ")";
    }
    stmt.execute(String.format("insert into \"%s\".\"%s\" values %s", schema, table, concat));
  }
```

## Cloudera Impala

### Put data to HDFS

Use following commands to put data into HDFS. Suppose the tpch1g data is stored in `/home/username/workspace/tpch1g` and you hope to put your data in `/tmp/tpch1g` in HDFS.
```bash
$ hdfs dfs -mkdir -p /tmp/tpch1g/region
$ hdfs dfs -mkdir -p /tmp/tpch1g/nation
$ hdfs dfs -mkdir -p /tmp/tpch1g/customer
$ hdfs dfs -mkdir -p /tmp/tpch1g/supplier
$ hdfs dfs -mkdir -p /tmp/tpch1g/part
$ hdfs dfs -mkdir -p /tmp/tpch1g/partsupp
$ hdfs dfs -mkdir -p /tmp/tpch1g/orders
$ hdfs dfs -mkdir -p /tmp/tpch1g/lineitem
$ hdfs dfs -put /home/username/workspace/tpch1g/region/region.tbl       /tmp/tpch1g/region
$ hdfs dfs -put /home/username/workspace/tpch1g/nation/nation.tbl       /tmp/tpch1g/nation
$ hdfs dfs -put /home/username/workspace/tpch1g/customer/customer.tbl   /tmp/tpch1g/customer
$ hdfs dfs -put /home/username/workspace/tpch1g/supplier/supplier.tbl   /tmp/tpch1g/supplier
$ hdfs dfs -put /home/username/workspace/tpch1g/part/part.tbl           /tmp/tpch1g/part
$ hdfs dfs -put /home/username/workspace/tpch1g/partsupp/partsupp.tbl   /tmp/tpch1g/partsupp
$ hdfs dfs -put /home/username/workspace/tpch1g/orders/orders.tbl       /tmp/tpch1g/orders
$ hdfs dfs -put /home/username/workspace/tpch1g/lineitem/lineitem.tbl   /tmp/tpch1g/lineitem
```

If you encounter write permission problem in the next step when creating tables, you can use command
```bash
$ hdfs dfs -chmod -R 777 /tmp/tpch1g
```
to give full access to your directory.


### Create tables and load data

Connect to Impala.

```bash
$ impala-shell
```

Create a schema for testing.

```
create schema `tpch1g`;
```

Create tables and load data. Simply copy and paste the following table definition statements into the Impala shell.

```sql
-- nation
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`nation` (
  `n_nationkey`  INT,
  `n_name`       STRING,
  `n_regionkey`  INT,
  `n_comment`    STRING,
  `n_dummy`      STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/nation';

-- region
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`region` (
  `r_regionkey`  INT,
  `r_name`       STRING,
  `r_comment`    STRING,
  `r_dummy`      STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/region';

-- supplier
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`supplier` (
  `s_suppkey`     INT,
  `s_name`        STRING,
  `s_address`     STRING,
  `s_nationkey`   INT,
  `s_phone`       STRING,
  `s_acctbal`     DECIMAL(15,2),
  `s_comment`     STRING,
  `s_dummy`       STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/supplier';

-- customer
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`customer` (
  `c_custkey`     INT,
  `c_name`        STRING,
  `c_address`     STRING,
  `c_nationkey`   INT,
  `c_phone`       STRING,
  `c_acctbal`     DECIMAL(15,2),
  `c_mktsegment`  STRING,
  `c_comment`     STRING,
  `c_dummy`       STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/customer';

-- part
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`part` (
  `p_partkey`     INT,
  `p_name`        STRING,
  `p_mfgr`        STRING,
  `p_brand`       STRING,
  `p_type`        STRING,
  `p_size`        INT,
  `p_container`   STRING,
  `p_retailprice` DECIMAL(15,2) ,
  `p_comment`     STRING ,
  `p_dummy`       STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/part';

-- partsupp
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`partsupp` (
  `ps_partkey`     INT,
  `ps_suppkey`     INT,
  `ps_availqty`    INT,
  `ps_supplycost`  DECIMAL(15,2),
  `ps_comment`     STRING,
  `ps_dummy`       STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/partsupp';

-- orders
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`orders` (
  `o_orderkey`       INT,
  `o_custkey`        INT,
  `o_orderstatus`    STRING,
  `o_totalprice`     DECIMAL(15,2),
  `o_orderdate`      TIMESTAMP,
  `o_orderpriority`  STRING,
  `o_clerk`          STRING,
  `o_shippriority`   INT,
  `o_comment`        STRING,
  `o_dummy`          STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/orders';

-- lineitem
CREATE EXTERNAL TABLE IF NOT EXISTS `tpch1g`.`lineitem`(
  `l_orderkey`          INT,
  `l_partkey`           INT,
  `l_suppkey`           INT,
  `l_linenumber`        INT,
  `l_quantity`          DECIMAL(15,2),
  `l_extendedprice`     DECIMAL(15,2),
  `l_discount`          DECIMAL(15,2),
  `l_tax`               DECIMAL(15,2),
  `l_returnflag`        STRING,
  `l_linestatus`        STRING,
  `l_shipdate`          TIMESTAMP,
  `l_commitdate`        TIMESTAMP,
  `l_receiptdate`       TIMESTAMP,
  `l_shipinstruct`      STRING,
  `l_shipmode`          STRING,
  `l_comment`           STRING,
  `l_dummy`             STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
  LOCATION '/tmp/tpch1g/lineitem';
```
