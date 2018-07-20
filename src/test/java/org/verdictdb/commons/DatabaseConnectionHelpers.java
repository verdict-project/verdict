package org.verdictdb.commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.spark.sql.SparkSession;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
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
    spark.conf().set("spark.cores.max", "24");
    spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    spark.conf().set("spark.sql.tungsten.enabled", "true");
    spark.conf().set("spark.eventLog.enabled", "true");
    spark.conf().set("spark.app.id", "YourApp");
    spark.conf().set("spark.io.compression.codec", "snappy");
    spark.conf().set("spark.rdd.compress", "true");
    spark.conf().set("spark.streaming.backpressure.enabled", "true");
    spark.conf().set("spark.kryoserializer.buffer.max", "1");
    spark.conf().set("spark.default.parallelism", "1");
    spark.conf().set("spark.executor.cores", "8");
    spark.conf().set("spark.shuffle.sort.bypassMergeThreshold", "50");
    spark.conf().set("spark.broadcast.blockSize", "1");
    spark.conf().set("spark.sql.parquet.compression.codec", "snappy");
    spark.conf().set("spark.sql.parquet.mergeSchema", "true");
    spark.conf().set("spark.sql.parquet.binaryAsString", "true");
    spark.conf().set("spark.sql.crossJoin.enabled", "true");
    // create schema
    spark.sql(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", schema));
    spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));
    // create tables
    String datafilePath = new File("src/test/resources/tpch_test_data/").getAbsolutePath();
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
        "  `l_comment`        VARCHAR(44), " +
        "  `l_dummy`          VARCHAR(10))" +
        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' " +
        "STORED AS textfile " +
        "LOCATION '%s/lineitem'",
          schema, datafilePath));

    return spark;
  }

  /*
  public static Connection setupImpala(
      String connectionString, String user, String password, String schema)
      throws VerdictDBDbmsException, SQLException, IOException {

    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    dbmsConn.execute("USE default");
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`nation`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`region`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`supplier`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`customer`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`part`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`partsupp`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem`", schema));
    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s`", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));
    dbmsConn.execute(String.format("USE `%s`", schema));

    // Create tables
    dbmsConn.execute(String.format(
        "CREATE EXTERNAL TABLE  IF NOT EXISTS `%s`.`nation` (" +
            "  `n_nationkey`  INT, " +
            "  `n_name`       CHAR(25), " +
            "  `n_regionkey`  INT, " +
            "  `n_comment`    STRING, " +
            "  `n_dummy`      STRING " +
            "  )",
        schema));
    File file = new File("src/test/resources/tpch_test_data/nation/nation.tbl");
    String content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      values[1] = "'" + values[1]+ "'";
      values[3] = "'" + values[3]+ "'";
      values[4] = "''";
      row = values[0]+","+values[1]+","+values[2]+","+values[3]+","+"''";
      dbmsConn.execute(String.format("insert into `%s`.`nation` values (%s)", schema, row));
    }
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`region`  (" +
            "  `r_regionkey`  INT, " +
            "  `r_name`       CHAR(25), " +
            "  `r_comment`    STRING, " +
            "  `r_dummy`      STRING " +
            "  )",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`part`  ( `p_partkey`     INT, " +
            "  `p_name`        STRING, " +
            "  `p_mfgr`        CHAR(25), " +
            "  `p_brand`       CHAR(10), " +
            "  `p_type`        STRING, " +
            "  `p_size`        INT, " +
            "  `p_container`   CHAR(10), " +
            "  `p_retailprice` DECIMAL(15,2) , " +
            "  `p_comment`     STRING , " +
            "  `p_dummy`       STRING " +
            "  )",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`supplier` ( " +
            "  `s_suppkey`     INT , " +
            "  `s_name`        CHAR(25) , " +
            "  `s_address`     STRING , " +
            "  `s_nationkey`   INT , " +
            "  `s_phone`       CHAR(15) , " +
            "  `s_acctbal`     DECIMAL(15,2) , " +
            "  `s_comment`     STRING, " +
            "  `s_dummy` STRING " +
            "  )",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`partsupp` ( " +
            "  `ps_partkey`     INT , " +
            "  `ps_suppkey`     INT , " +
            "  `ps_availqty`    INT , " +
            "  `ps_supplycost`  DECIMAL(15,2)  , " +
            "  `ps_comment`     STRING, " +
            "  `ps_dummy`       STRING " +
            "  )",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`customer` (" +
            "  `c_custkey`     INT , " +
            "  `c_name`        STRING , " +
            "  `c_address`     STRING , " +
            "  `c_nationkey`   INT , " +
            "  `c_phone`       CHAR(15) , " +
            "  `c_acctbal`     DECIMAL(15,2)   , " +
            "  `c_mktsegment`  CHAR(10) , " +
            "  `c_comment`     STRING, " +
            "  `c_dummy`       STRING " +
            "  )",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS  `%s`.`orders`  ( " +
            "  `o_orderkey`       INT , " +
            "  `o_custkey`        INT , " +
            "  `o_orderstatus`    CHAR(1) , " +
            "  `o_totalprice`     DECIMAL(15,2) , " +
            "  `o_orderdate`      TIMESTAMP , " +
            "  `o_orderpriority`  CHAR(15) , " +
            "  `o_clerk`          CHAR(15) , " +
            "  `o_shippriority`   INT , " +
            "  `o_comment`        STRING, " +
            "  `o_dummy` STRING " +
            "  )",
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
            "  `l_shipdate`    TIMESTAMP , " +
            "  `l_commitdate`  TIMESTAMP , " +
            "  `l_receiptdate` TIMESTAMP , " +
            "  `l_shipinstruct` CHAR(25) , " +
            "  `l_shipmode`     CHAR(10) , " +
            "  `l_comment`      STRING, " +
            "  `l_dummy` STRING)",
        schema));

    // Load data
    dbmsConn.execute(String.format("LOAD DATA INPATH 'src/test/resources/tpch_test_data/region/region.tbl' " +
        "INTO TABLE `%s`.`region`", schema));
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
  */
  static String getQuoted(String value) {
    return "'"+value+"'";
  }

  public static Connection setupRedshift(
      String connectionString, String user, String password, String schema)
      throws VerdictDBDbmsException, SQLException, IOException {
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
            "  PRIMARY KEY (\"ps_partkey\", \"ps_suppkey\"))",
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
    // load data use insert
    String concat = "";
    File file = new File("src/test/resources/tpch_test_data/nation/nation.tbl");
    String content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      values[1] = "'" + values[1]+ "'";
      values[3] = "'" + values[3]+ "'";
      values[4] = "''";
      row = values[0]+","+values[1]+","+values[2]+","+values[3]+","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"nation\" values %s", schema, concat));

    concat = "";
    file = new File("src/test/resources/tpch_test_data/region/region.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      values[1] = "'" + values[1]+ "'";
      values[2] = "'" + values[2]+ "'";
      row = values[0]+","+values[1]+","+values[2]+","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"region\" values %s", schema, concat));

    concat="";
    file = new File("src/test/resources/tpch_test_data/part/part.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = values[0]+","+getQuoted(values[1])+","+getQuoted(values[2])+","
          +getQuoted(values[3])+","+getQuoted(values[4])+","+values[5]+","+getQuoted(values[6])
          +","+values[7]+","+getQuoted(values[8])+","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"part\" values %s", schema, concat));

    concat = "";
    file = new File("src/test/resources/tpch_test_data/supplier/supplier.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = values[0]+","+getQuoted(values[1])+","+getQuoted(values[2])+","
          +values[3]+","+getQuoted(values[4])+","+values[5]+","+getQuoted(values[6])
          + ","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"supplier\" values %s", schema, concat));

    concat = "";
    file = new File("src/test/resources/tpch_test_data/partsupp/partsupp.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = values[0]+","+values[1]+","+values[2]+","
          +values[3]+","+getQuoted(values[4])
          + ","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"partsupp\" values %s", schema, concat));

    concat = "";
    file = new File("src/test/resources/tpch_test_data/customer/customer.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = values[0]+","+getQuoted(values[1])+","+getQuoted(values[2])+","
          +values[3]+","+getQuoted(values[4])+","+values[5]+","+getQuoted(values[6])
          +","+getQuoted(values[7])+","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"customer\" values %s", schema, concat));

    concat = "";
    file = new File("src/test/resources/tpch_test_data/orders/orders.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = values[0]+","+values[1]+","+getQuoted(values[2])+","
          +values[3]+","+getQuoted(values[4])+","+getQuoted(values[5])+","+getQuoted(values[6])
          +","+values[7]+","+getQuoted(values[8])+","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"orders\" values %s", schema, concat));

    concat = "";
    file = new File("src/test/resources/tpch_test_data/lineitem/lineitem.tbl");
    content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = values[0]+","+values[1]+","+values[2]+","
          +values[3]+","+values[4]+","+values[5]+","+values[6]
          +","+values[7]+","+getQuoted(values[8])+","+getQuoted(values[9])
          +","+getQuoted(values[10])+","+getQuoted(values[11])
          +","+getQuoted(values[12])+","+getQuoted(values[13])
          +","+getQuoted(values[14])+","+getQuoted(values[15])+","+"''";
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      }
      else concat = concat + "," + "(" + row + ")";
    }
    dbmsConn.execute(String.format("insert into \"%s\".\"lineitem\" values %s", schema, concat));
    return conn;
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
      throws VerdictDBDbmsException, SQLException, IOException {
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
            "  PRIMARY KEY (\"ps_partkey\", \"ps_suppkey\"))",
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

    CopyManager copy = new CopyManager((BaseConnection) conn);
    File region = new File("src/test/resources/tpch_test_data/region/region.tbl");
    InputStream in = new FileInputStream(region);
    copy.copyIn(String.format("COPY \"%s\".\"region\" FROM STDOUT DELIMITER '|'", schema), in);
    File nation = new File("src/test/resources/tpch_test_data/nation/nation.tbl");
    in = new FileInputStream(nation);
    copy.copyIn(String.format("COPY \"%s\".\"nation\" FROM STDOUT DELIMITER '|'", schema), in);
    File supplier = new File("src/test/resources/tpch_test_data/supplier/supplier.tbl");
    in = new FileInputStream(supplier);
    copy.copyIn(String.format("COPY \"%s\".\"supplier\" FROM STDOUT DELIMITER '|'", schema), in);
    File customer = new File("src/test/resources/tpch_test_data/customer/customer.tbl");
    in = new FileInputStream(customer);
    copy.copyIn(String.format("COPY \"%s\".\"customer\" FROM STDOUT DELIMITER '|'", schema), in);
    File part = new File("src/test/resources/tpch_test_data/part/part.tbl");
    in = new FileInputStream(part);
    copy.copyIn(String.format("COPY \"%s\".\"part\" FROM STDOUT DELIMITER '|'", schema), in);
    File partsupp = new File("src/test/resources/tpch_test_data/partsupp/partsupp.tbl");
    in = new FileInputStream(partsupp);
    copy.copyIn(String.format("COPY \"%s\".\"partsupp\" FROM STDOUT DELIMITER '|'", schema), in);
    File lineitem = new File("src/test/resources/tpch_test_data/lineitem/lineitem.tbl");
    in = new FileInputStream(lineitem);
    copy.copyIn(String.format("COPY \"%s\".\"lineitem\" FROM STDOUT DELIMITER '|'", schema), in);
    File orders = new File("src/test/resources/tpch_test_data/orders/orders.tbl");
    in = new FileInputStream(orders);
    copy.copyIn(String.format("COPY \"%s\".\"orders\" FROM STDOUT DELIMITER '|'", schema), in);

    return conn;
  }

}
