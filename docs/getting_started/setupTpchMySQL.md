# Set up TPC-H table in MySQL

You can verify VerdictDB through TPC-H queries. Here are the steps of how to set up TPC-H tables in MySQL database.


### 1. Download TPC-H test data

In your terminal, type command
```
curl http://dbgroup-internal.eecs.umich.edu/projects/verdictdb/tpch1g.zip -o tpch1g.zip
```
to download our TPC-H test data. The data size is 1 gigabyte.


### 2. Unzip TPC-H test data

Unzip the file you have downloaded and put in the directory you want to be loaded later.

### 3. Create table

Now, to set up TPC-H table in MySQL, you can either directly issue queries in MySQL client using queries given below. Suppose we want to setup
our tables in schema `tpch1g`.

* Table Nation
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`nation`(
  `n_nationkey`  INT,
  `n_name`       CHAR(25),
  `n_regionkey`  INT,
  `n_comment`    VARCHAR(152),
  `n_dummy`      VARCHAR(10),
  PRIMARY KEY (`n_nationkey`));
```
* Table Region
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`region`(
  `r_regionkey`  INT,
  `r_name`       CHAR(25),
  `r_comment`    VARCHAR(152),
  `r_dummy`      VARCHAR(10),
  PRIMARY KEY (`r_regionkey`));
```
* Table Supplier
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`supplier`(
  `s_suppkey`     INT,
  `s_name`        CHAR(25),
  `s_address`     VARCHAR(40),
  `s_nationkey`   INT,
  `s_phone`       CHAR(15),
  `s_acctbal`     DECIMAL(15,2),
  `s_comment`     VARCHAR(101),
  `s_dummy` varchar(10),
  PRIMARY KEY (`s_suppkey`));
```
* Table Customer
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`customer`(
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
```
* Table Part
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`part`(
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
```
* Table Partsupp
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`partsupp`(
  `ps_partkey`     INT,
  `ps_suppkey`     INT,
  `ps_availqty`    INT,
  `ps_supplycost`  DECIMAL(15,2),
  `ps_comment`     VARCHAR(199),
  `ps_dummy`       VARCHAR(10),
  PRIMARY KEY (`ps_partkey`));
```
* Table Orders
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`orders`(
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
```
* Table Lineitem
```sql
CREATE TABLE IF NOT EXISTS `tpch1g`.`lineitem`(
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

### 4. Load data into tables
After creating tables, we can load data into tables. Suppose you stored your data in '/path/resources'. Then you can use following
queries to load.
```
LOAD DATA LOCAL INFILE '/path/resources/region/region.tbl'     INTO TABLE `tpch1g`.`region`     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/nation/nation.tbl'     INTO TABLE `tpch1g`.`nation`     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/customer/customer.tbl' INTO TABLE `tpch1g`.`customer`   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/supplier/supplier.tbl' INTO TABLE `tpch1g`.`supplier`   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/part/part.tbl'         INTO TABLE `tpch1g`.`part`       FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/partsupp/partsupp.tbl' INTO TABLE `tpch1g`.`partsupp`   FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/orders/orders.tbl'     INTO TABLE `tpch1g`.`orders`     FIELDS TERMINATED BY '|';
LOAD DATA LOCAL INFILE '/path/resources/lineitem/lineitem.tbl' INTO TABLE `tpch1g`.`lineitem`   FIELDS TERMINATED BY '|';
```

