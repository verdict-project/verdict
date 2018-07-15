package org.verdictdb.commons;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.spark.sql.SparkSession;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;

public class DatabaseConnectionHelpers {
  
  public static SparkSession setupSpark(String appname, String schema) {
    SparkSession spark = SparkSession.builder().appName(appname)
        .master("local")
        .enableHiveSupport()
        .getOrCreate();
    
    // create schema
    spark.sql(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", schema));
    spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));
    
    // create tables
    String datafilePath = new File("src/test/resources/tpch_test_data/").getAbsolutePath();
    System.out.println(datafilePath);
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`nation` (" +
        "  `n_nationkey`  INT, " +
        "  `n_name`       CHAR(25), " +
        "  `n_regionkey`  INT, " +
        "  `n_comment`    VARCHAR(152), " +
        "  `n_dummy`      VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS TEXTFILE " +
        "LOCATION '%s/nation'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`region` (" +
        "  `r_regionkey`  INT, " +
        "  `r_name`       CHAR(25), " +
        "  `r_comment`    VARCHAR(152), " +
        "  `r_dummy`      VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS TEXTFILE " +
        "LOCATION '%s/region'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`part`  (" +
        "  `p_partkey`     INT, " +
        "  `p_name`        VARCHAR(55), " +
        "  `p_mfgr`        CHAR(25), " +
        "  `p_brand`       CHAR(10), " +
        "  `p_type`        VARCHAR(25), " +
        "  `p_size`        INT, " +
        "  `p_container`   CHAR(10), " +
        "  `p_retailprice` DECIMAL(15,2) , " +
        "  `p_comment`     VARCHAR(23) , " +
        "  `p_dummy`       VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/part'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`supplier` ( " +
        "  `s_suppkey`     INT , " +
        "  `s_name`        CHAR(25) , " +
        "  `s_address`     VARCHAR(40) , " +
        "  `s_nationkey`   INT , " +
        "  `s_phone`       CHAR(15) , " +
        "  `s_acctbal`     DECIMAL(15,2) , " +
        "  `s_comment`     VARCHAR(101), " +
        "  `s_dummy`       VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/supplier'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`partsupp` ( " +
        "  `ps_partkey`     INT , " +
        "  `ps_suppkey`     INT , " +
        "  `ps_availqty`    INT , " +
        "  `ps_supplycost`  DECIMAL(15,2)  , " +
        "  `ps_comment`     VARCHAR(199), " +
        "  `ps_dummy`       VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/partsupp'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`customer` (" +
        "  `c_custkey`     INT , " +
        "  `c_name`        VARCHAR(25) , " +
        "  `c_address`     VARCHAR(40) , " +
        "  `c_nationkey`   INT , " +
        "  `c_phone`       CHAR(15) , " +
        "  `c_acctbal`     DECIMAL(15,2)   , " +
        "  `c_mktsegment`  CHAR(10) , " +
        "  `c_comment`     VARCHAR(117), " +
        "  `c_dummy`       VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/customer'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS  `%s`.`orders`  ( " +
        "  `o_orderkey`       INT , " +
        "  `o_custkey`        INT , " +
        "  `o_orderstatus`    CHAR(1) , " +
        "  `o_totalprice`     DECIMAL(15,2) , " +
        "  `o_orderdate`      DATE , " +
        "  `o_orderpriority`  CHAR(15) , " +
        "  `o_clerk`          CHAR(15) , " +
        "  `o_shippriority`   INT , " +
        "  `o_comment`        VARCHAR(79), " +
        "  `o_dummy`          VARCHAR(10)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/orders'",
          schema, datafilePath));
    spark.sql(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`lineitem` (" + 
        "  `l_orderkey`       INT , " +
        "  `l_partkey`        INT , " +
        "  `l_suppkey`        INT , " +
        "  `l_linenumber`     INT , " +
        "  `l_quantity`       DECIMAL(15,2) , " +
        "  `l_extendedprice`  DECIMAL(15,2) , " +
        "  `l_discount`       DECIMAL(15,2) , " +
        "  `l_tax`            DECIMAL(15,2) , " +
        "  `l_returnflag`     CHAR(1) , " +
        "  `l_linestatus`     CHAR(1) , " +
        "  `l_shipdate`       DATE , " +
        "  `l_commitdate`     DATE , " +
        "  `l_receiptdate`    DATE , " +
        "  `l_shipinstruct`   CHAR(25) , " +
        "  `l_shipmode`       CHAR(10) , " +
        "  `l_comment`        VARCHAR(44)) " +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/lineitem'",
          schema, datafilePath));
    
    return spark;
  }

  public static Connection setupMySql(
      String connectionString, String user, String password, String schema)
          throws VerdictDBDbmsException, SQLException {

    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s`", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));

    // Create tables
    dbmsConn.execute(String.format(
      "CREATE TABLE  IF NOT EXISTS `%s`.`nation` (" +
      "  `n_nationkey`  INT, " +
      "  `n_name`       CHAR(25), " +
      "  `n_regionkey`  INT, " +
      "  `n_comment`    VARCHAR(152), " +
      "  `n_dummy`      VARCHAR(10), " +
      "  PRIMARY KEY (`n_nationkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE  IF NOT EXISTS `%s`.`region`  (" +
      "  `r_regionkey`  INT, " +
      "  `r_name`       CHAR(25), " +
      "  `r_comment`    VARCHAR(152), " +
      "  `r_dummy`      VARCHAR(10), " +
      "  PRIMARY KEY (`r_regionkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE  IF NOT EXISTS `%s`.`part`  ( `p_partkey`     INT, " +
      "  `p_name`        VARCHAR(55), " +
      "  `p_mfgr`        CHAR(25), " +
      "  `p_brand`       CHAR(10), " +
      "  `p_type`        VARCHAR(25), " +
      "  `p_size`        INT, " +
      "  `p_container`   CHAR(10), " +
      "  `p_retailprice` DECIMAL(15,2) , " +
      "  `p_comment`     VARCHAR(23) , " +
      "  `p_dummy`       VARCHAR(10), " +
      "  PRIMARY KEY (`p_partkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE  IF NOT EXISTS `%s`.`supplier` ( " +
      "  `s_suppkey`     INT , " +
      "  `s_name`        CHAR(25) , " +
      "  `s_address`     VARCHAR(40) , " +
      "  `s_nationkey`   INT , " +
      "  `s_phone`       CHAR(15) , " +
      "  `s_acctbal`     DECIMAL(15,2) , " +
      "  `s_comment`     VARCHAR(101), " +
      "  `s_dummy` varchar(10), " +
      "  PRIMARY KEY (`s_suppkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE  IF NOT EXISTS `%s`.`partsupp` ( " +
      "  `ps_partkey`     INT , " +
      "  `ps_suppkey`     INT , " +
      "  `ps_availqty`    INT , " +
      "  `ps_supplycost`  DECIMAL(15,2)  , " +
      "  `ps_comment`     VARCHAR(199), " +
      "  `ps_dummy`       VARCHAR(10), " +
      "  PRIMARY KEY (`ps_partkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE  IF NOT EXISTS `%s`.`customer` (" +
      "  `c_custkey`     INT , " +
      "  `c_name`        VARCHAR(25) , " +
      "  `c_address`     VARCHAR(40) , " +
      "  `c_nationkey`   INT , " +
      "  `c_phone`       CHAR(15) , " +
      "  `c_acctbal`     DECIMAL(15,2)   , " +
      "  `c_mktsegment`  CHAR(10) , " +
      "  `c_comment`     VARCHAR(117), " +
      "  `c_dummy`       VARCHAR(10), " +
      "  PRIMARY KEY (`c_custkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE IF NOT EXISTS  `%s`.`orders`  ( " +
      "  `o_orderkey`       INT , " +
      "  `o_custkey`        INT , " +
      "  `o_orderstatus`    CHAR(1) , " +
      "  `o_totalprice`     DECIMAL(15,2) , " +
      "  `o_orderdate`      DATE , " +
      "  `o_orderpriority`  CHAR(15) , " +
      "  `o_clerk`          CHAR(15) , " +
      "  `o_shippriority`   INT , " +
      "  `o_comment`        VARCHAR(79), " +
      "  `o_dummy` varchar(10), " +
      "  PRIMARY KEY (`o_orderkey`))",
        schema));
    dbmsConn.execute(String.format(
      "CREATE TABLE IF NOT EXISTS `%s`.`lineitem` ( `l_orderkey`    INT , " +
      "  `l_partkey`     INT , " +
      "  `l_suppkey`     INT , " +
      "  `l_linenumber`  INT , " +
      "  `l_quantity`    DECIMAL(15,2) , " +
      "  `l_extendedprice`  DECIMAL(15,2) , " +
      "  `l_discount`    DECIMAL(15,2) , " +
      "  `l_tax`         DECIMAL(15,2) , " +
      "  `l_returnflag`  CHAR(1) , " +
      "  `l_linestatus`  CHAR(1) , " +
      "  `l_shipdate`    DATE , " +
      "  `l_commitdate`  DATE , " +
      "  `l_receiptdate` DATE , " +
      "  `l_shipinstruct` CHAR(25) , " +
      "  `l_shipmode`     CHAR(10) , " +
      "  `l_comment`      VARCHAR(44), " +
      "  `l_dummy` varchar(10))",
        schema));

    // Load data
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/region/region.tbl' " +
        "INTO TABLE `%s`.`region` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/nation/nation.tbl' " +
        "INTO TABLE `%s`.`nation` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/supplier/supplier.tbl' " +
        "INTO TABLE `%s`.`supplier` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/customer/customer.tbl' " +
        "INTO TABLE `%s`.`customer` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/part/part.tbl' " +
        "INTO TABLE `%s`.`part` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/partsupp/partsupp.tbl' " +
        "INTO TABLE `%s`.`partsupp` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/lineitem/lineitem.tbl' " +
        "INTO TABLE `%s`.`lineitem` FIELDS TERMINATED BY '|'", schema));
    dbmsConn.execute(String.format("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/orders/orders.tbl' " +
        "INTO TABLE `%s`.`orders` FIELDS TERMINATED BY '|'", schema));

    return conn;
  }

  public static Connection setupPostgresql(String connectionString, String user, String password, String schema)
      throws VerdictDBDbmsException, SQLException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = new JdbcConnection(conn, new PostgresqlSyntax());

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));

    // Create tables
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS \"%s\".\"nation\" (" +
            "  \"n_nationkey\"  INT, " +
            "  \"n_name\"       CHAR(25), " +
            "  \"n_regionkey\"  INT, " +
            "  \"n_comment\"    VARCHAR(152), " +
            "  \"n_dummy\"      VARCHAR(10), " +
            "  PRIMARY KEY (\"n_nationkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS \"%s\".\"region\"  (" +
            "  \"r_regionkey\"  INT, " +
            "  \"r_name\"       CHAR(25), " +
            "  \"r_comment\"    VARCHAR(152), " +
            "  \"r_dummy\"      VARCHAR(10), " +
            "  PRIMARY KEY (\"r_regionkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS \"%s\".\"part\"  ( \"p_partkey\"     INT, " +
            "  \"p_name\"        VARCHAR(55), " +
            "  \"p_mfgr\"        CHAR(25), " +
            "  \"p_brand\"       CHAR(10), " +
            "  \"p_type\"        VARCHAR(25), " +
            "  \"p_size\"        INT, " +
            "  \"p_container\"   CHAR(10), " +
            "  \"p_retailprice\" DECIMAL(15,2) , " +
            "  \"p_comment\"     VARCHAR(23) , " +
            "  \"p_dummy\"       VARCHAR(10), " +
            "  PRIMARY KEY (\"p_partkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS \"%s\".\"supplier\" ( " +
            "  \"s_suppkey\"     INT , " +
            "  \"s_name\"        CHAR(25) , " +
            "  \"s_address\"     VARCHAR(40) , " +
            "  \"s_nationkey\"   INT , " +
            "  \"s_phone\"       CHAR(15) , " +
            "  \"s_acctbal\"     DECIMAL(15,2) , " +
            "  \"s_comment\"     VARCHAR(101), " +
            "  \"s_dummy\" varchar(10), " +
            "  PRIMARY KEY (\"s_suppkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS \"%s\".\"partsupp\" ( " +
            "  \"ps_partkey\"     INT , " +
            "  \"ps_suppkey\"     INT , " +
            "  \"ps_availqty\"    INT , " +
            "  \"ps_supplycost\"  DECIMAL(15,2)  , " +
            "  \"ps_comment\"     VARCHAR(199), " +
            "  \"ps_dummy\"       VARCHAR(10), " +
            "  PRIMARY KEY (\"ps_partkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS \"%s\".\"customer\" (" +
            "  \"c_custkey\"     INT , " +
            "  \"c_name\"        VARCHAR(25) , " +
            "  \"c_address\"     VARCHAR(40) , " +
            "  \"c_nationkey\"   INT , " +
            "  \"c_phone\"       CHAR(15) , " +
            "  \"c_acctbal\"     DECIMAL(15,2)   , " +
            "  \"c_mktsegment\"  CHAR(10) , " +
            "  \"c_comment\"     VARCHAR(117), " +
            "  \"c_dummy\"       VARCHAR(10), " +
            "  PRIMARY KEY (\"c_custkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS  \"%s\".\"orders\"  ( " +
            "  \"o_orderkey\"       INT , " +
            "  \"o_custkey\"        INT , " +
            "  \"o_orderstatus\"    CHAR(1) , " +
            "  \"o_totalprice\"     DECIMAL(15,2) , " +
            "  \"o_orderdate\"      DATE , " +
            "  \"o_orderpriority\"  CHAR(15) , " +
            "  \"o_clerk\"          CHAR(15) , " +
            "  \"o_shippriority\"   INT , " +
            "  \"o_comment\"        VARCHAR(79), " +
            "  \"o_dummy\" varchar(10), " +
            "  PRIMARY KEY (\"o_orderkey\"))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS \"%s\".\"lineitem\" (" +
            "  \"l_orderkey\"    INT , " +
            "  \"l_partkey\"     INT , " +
            "  \"l_suppkey\"     INT , " +
            "  \"l_linenumber\"  INT , " +
            "  \"l_quantity\"    DECIMAL(15,2) , " +
            "  \"l_extendedprice\"  DECIMAL(15,2) , " +
            "  \"l_discount\"    DECIMAL(15,2) , " +
            "  \"l_tax\"         DECIMAL(15,2) , " +
            "  \"l_returnflag\"  CHAR(1) , " +
            "  \"l_linestatus\"  CHAR(1) , " +
            "  \"l_shipdate\"    DATE , " +
            "  \"l_commitdate\"  DATE , " +
            "  \"l_receiptdate\" DATE , " +
            "  \"l_shipinstruct\" CHAR(25) , " +
            "  \"l_shipmode\"     CHAR(10) , " +
            "  \"l_comment\"      VARCHAR(44), " +
            "  \"l_dummy\" varchar(10))",
        schema));

    // Load data
    dbmsConn.execute(String.format("COPY \"%s\".\"region\" FROM 'src/test/resources/tpch_test_data/region/region.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"nation\" FROM 'src/test/resources/tpch_test_data/nation/nation.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"supplier\" FROM 'src/test/resources/tpch_test_data/supplier/supplier.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"customer\" FROM 'src/test/resources/tpch_test_data/customer/customer.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"part\" FROM 'src/test/resources/tpch_test_data/part/part.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"partsupp\" FROM 'src/test/resources/tpch_test_data/partsupp/partsupp.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"lineitem\" FROM 'src/test/resources/tpch_test_data/lineitem/lineitem.tbl' DELIMITER '|'", schema));
    dbmsConn.execute(String.format("COPY \"%s\".\"orders\" FROM 'src/test/resources/tpch_test_data/orders/orders.tbl' DELIMITER '|'", schema));

    return conn;
  }

}
