# TPC-H Data Setup

This is a step-by-step guide about how to set up TPC-H tables in different databases.


## Download 1GB Data

In your terminal, go to your work directory (say `/home/username/workspace`) and download the data we have archived for you:

```bash
cd /home/username/workspace
curl http://dbgroup-internal.eecs.umich.edu/projects/verdictdb/tpch1g.zip -o tpch1g.zip
```

Unzip the downloaded file:

```bash
unzip tpch1g.zip
```

It will create a new directory named `tpch1g` under your work directory. The directory contains 8 sub-directories for each of 8 tables.


## MySQL

### Create tables

Connect to your MySQL database.

```bash
mysql -uroot
```

Create a schema and set it as the default schema.

```bash
mysql> create database tpch1g;
mysql> use tpch1g;
```

Create empty tables; simply copy and paste the following table definition statements. We will import the data later into these tables.

```sql
-- nation
CREATE TABLE IF NOT EXISTS nation (
  `n_nationkey`  INT,
  `n_name`       CHAR(25),
  `n_regionkey`  INT,
  `n_comment`    VARCHAR(152),
  `n_dummy`      VARCHAR(10),
  PRIMARY KEY (`n_nationkey`));

-- region
CREATE TABLE IF NOT EXISTS region (
  `r_regionkey`  INT,
  `r_name`       CHAR(25),
  `r_comment`    VARCHAR(152),
  `r_dummy`      VARCHAR(10),
  PRIMARY KEY (`r_regionkey`));

-- supplier
CREATE TABLE IF NOT EXISTS supplier (
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
CREATE TABLE IF NOT EXISTS customer (
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
  `p_dummy`       VARCHAR(10),
  PRIMARY KEY (`p_partkey`));

-- partsupp
CREATE TABLE IF NOT EXISTS partsupp (
  `ps_partkey`     INT,
  `ps_suppkey`     INT,
  `ps_availqty`    INT,
  `ps_supplycost`  DECIMAL(15,2),
  `ps_comment`     VARCHAR(199),
  `ps_dummy`       VARCHAR(10),
  PRIMARY KEY (`ps_partkey`));

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
  `o_dummy`          VARCHAR(10),
  PRIMARY KEY (`o_orderkey`));

-- lineitem
CREATE TABLE IF NOT EXISTS lineitem`(
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

Suppose your work directory is `/home/username/workspace` and the tpch1g data is stored in `/home/username/workspace/tpch1g`. Then use the following commands to load the data.

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


## Apache Spark


## Redshift


## Cloudera Impala

