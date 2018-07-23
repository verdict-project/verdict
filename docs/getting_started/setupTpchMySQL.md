# Set up TPC-H table in MySQL

You can verify VerdictDB through TPC-H queries. Here are the steps of how to set up TPC-H tables in MySQL database.


### 1. Download TPC-H test data

In your terminal, type command
```
curl http://dbgroup-internal.eecs.umich.edu/projects/verdictdb/tpch1g.zip -o tpch1g.zip
```
to download our TPC-H test data. The data size is 1 gigabyte.


### 2. Put TPC-H test data into your project

Unzip the file you have downloaded. You can create a directory, such as `src/test/resources` and put the unzipped file into your java project.

### 3. Create table and load data

Now, to set up TPC-H table in MySQL, you can either directly issue queries in MySQL terminal or through MySQL JDBC driver.
We are using JDBC to set up TPC-H table. You can use following function.

```java
 public Connection setupMySql(String user, String password, String schema, String path)
      throws SQLException {

    String mysqlConnectionString = "jdbc:mysql://root?autoReconnect=true&useSSL=false";
    Connection conn = DriverManager.getConnection(mysqlConnectionString, user, password);
    Statement stmt = conn.createStatement();

    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));

    // Create tables
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`nation` ("
                + "  `n_nationkey`  INT, "
                + "  `n_name`       CHAR(25), "
                + "  `n_regionkey`  INT, "
                + "  `n_comment`    VARCHAR(152), "
                + "  `n_dummy`      VARCHAR(10), "
                + "  PRIMARY KEY (`n_nationkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`region`  ("
                + "  `r_regionkey`  INT, "
                + "  `r_name`       CHAR(25), "
                + "  `r_comment`    VARCHAR(152), "
                + "  `r_dummy`      VARCHAR(10), "
                + "  PRIMARY KEY (`r_regionkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`part`  ( `p_partkey`     INT, "
                + "  `p_name`        VARCHAR(55), "
                + "  `p_mfgr`        CHAR(25), "
                + "  `p_brand`       CHAR(10), "
                + "  `p_type`        VARCHAR(25), "
                + "  `p_size`        INT, "
                + "  `p_container`   CHAR(10), "
                + "  `p_retailprice` DECIMAL(15,2) , "
                + "  `p_comment`     VARCHAR(23) , "
                + "  `p_dummy`       VARCHAR(10), "
                + "  PRIMARY KEY (`p_partkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`supplier` ( "
                + "  `s_suppkey`     INT , "
                + "  `s_name`        CHAR(25) , "
                + "  `s_address`     VARCHAR(40) , "
                + "  `s_nationkey`   INT , "
                + "  `s_phone`       CHAR(15) , "
                + "  `s_acctbal`     DECIMAL(15,2) , "
                + "  `s_comment`     VARCHAR(101), "
                + "  `s_dummy` varchar(10), "
                + "  PRIMARY KEY (`s_suppkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`partsupp` ( "
                + "  `ps_partkey`     INT , "
                + "  `ps_suppkey`     INT , "
                + "  `ps_availqty`    INT , "
                + "  `ps_supplycost`  DECIMAL(15,2)  , "
                + "  `ps_comment`     VARCHAR(199), "
                + "  `ps_dummy`       VARCHAR(10), "
                + "  PRIMARY KEY (`ps_partkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`customer` ("
                + "  `c_custkey`     INT , "
                + "  `c_name`        VARCHAR(25) , "
                + "  `c_address`     VARCHAR(40) , "
                + "  `c_nationkey`   INT , "
                + "  `c_phone`       CHAR(15) , "
                + "  `c_acctbal`     DECIMAL(15,2)   , "
                + "  `c_mktsegment`  CHAR(10) , "
                + "  `c_comment`     VARCHAR(117), "
                + "  `c_dummy`       VARCHAR(10), "
                + "  PRIMARY KEY (`c_custkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS  `%s`.`orders`  ( "
                + "  `o_orderkey`       INT , "
                + "  `o_custkey`        INT , "
                + "  `o_orderstatus`    CHAR(1) , "
                + "  `o_totalprice`     DECIMAL(15,2) , "
                + "  `o_orderdate`      DATE , "
                + "  `o_orderpriority`  CHAR(15) , "
                + "  `o_clerk`          CHAR(15) , "
                + "  `o_shippriority`   INT , "
                + "  `o_comment`        VARCHAR(79), "
                + "  `o_dummy` varchar(10), "
                + "  PRIMARY KEY (`o_orderkey`))",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`lineitem` ( `l_orderkey`    INT , "
                + "  `l_partkey`     INT , "
                + "  `l_suppkey`     INT , "
                + "  `l_linenumber`  INT , "
                + "  `l_quantity`    DECIMAL(15,2) , "
                + "  `l_extendedprice`  DECIMAL(15,2) , "
                + "  `l_discount`    DECIMAL(15,2) , "
                + "  `l_tax`         DECIMAL(15,2) , "
                + "  `l_returnflag`  CHAR(1) , "
                + "  `l_linestatus`  CHAR(1) , "
                + "  `l_shipdate`    DATE , "
                + "  `l_commitdate`  DATE , "
                + "  `l_receiptdate` DATE , "
                + "  `l_shipinstruct` CHAR(25) , "
                + "  `l_shipmode`     CHAR(10) , "
                + "  `l_comment`      VARCHAR(44), "
                + "  `l_dummy` varchar(10))",
            schema));

    // Load data
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/region/region.tbl' "
                + "INTO TABLE `%s`.`region` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/nation/nation.tbl' "
                + "INTO TABLE `%s`.`nation` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/supplier/supplier.tbl' "
                + "INTO TABLE `%s`.`supplier` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/customer/customer.tbl' "
                + "INTO TABLE `%s`.`customer` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/part/part.tbl' "
                + "INTO TABLE `%s`.`part` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/partsupp/partsupp.tbl' "
                + "INTO TABLE `%s`.`partsupp` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/lineitem/lineitem.tbl' "
                + "INTO TABLE `%s`.`lineitem` FIELDS TERMINATED BY '|'",
            path, schema));
    stmt.execute(
        String.format(
            "LOAD DATA LOCAL INFILE '%s/tpch1g/orders/orders.tbl' "
                + "INTO TABLE `%s`.`orders` FIELDS TERMINATED BY '|'",
            path, schema));

    return conn;
  }
```

* `schema` is the database you want to set up your TPC-H table in.
* `user` and `password` is the username and password of MySQL. For instance, if your MySQL username is `root` and password is `rootpassword`,
then `user="root"` and `password="rootpassword"`.
* `mysqlConnectionString` is the MySQL connection URL.
* `path` is the infile path of the directory you stored your TPC-H test data. For instance, if you store the data in
`src/test/resources`, then `path = "src/test/resources"`
* You can also set up tables manually using the commands in this script.
