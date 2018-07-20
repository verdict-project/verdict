package org.verdictdb.commons;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.spark.sql.SparkSession;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.ImpalaSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;

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

  public static Connection setupImpala(
      String connectionString, String user, String password, String schema)
      throws SQLException, VerdictDBDbmsException {

    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    // CASCADE does not work in our version
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`nation`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`region`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`part`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`supplier`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`partsupp`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`customer`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem`", schema));

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s`", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));

    // Create tables
    dbmsConn.execute(String.format(
        "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`nation` (" +
            "  `n_nationkey`  INT, " +
            "  `n_name`       STRING, " +
            "  `n_regionkey`  INT, " +
            "  `n_comment`    STRING, " +
            "  `n_dummy`      STRING) " +
            "LOCATION '/tmp/tpch_test_data/nation'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`region`  (" +
            "  `r_regionkey`  INT, " +
            "  `r_name`       STRING, " +
            "  `r_comment`    STRING, " +
            "  `r_dummy`      STRING) " +
            "LOCATION '/tmp/tpch_test_data/region'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`part`  ( " +
            "  `p_partkey`     INT, " +
            "  `p_name`        STRING, " +
            "  `p_mfgr`        STRING, " +
            "  `p_brand`       STRING, " +
            "  `p_type`        STRING, " +
            "  `p_size`        INT, " +
            "  `p_container`   STRING, " +
            "  `p_retailprice` DECIMAL(15,2) , " +
            "  `p_comment`     STRING, " +
            "  `p_dummy`       STRING) " +
            "LOCATION '/tmp/tpch_test_data/part'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`supplier` ( " +
            "  `s_suppkey`     INT , " +
            "  `s_name`        STRING , " +
            "  `s_address`     STRING, " +
            "  `s_nationkey`   INT , " +
            "  `s_phone`       STRING , " +
            "  `s_acctbal`     DECIMAL(15,2) , " +
            "  `s_comment`     STRING, " +
            "  `s_dummy`       STRING) " +
            "LOCATION '/tmp/tpch_test_data/supplier'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`partsupp` ( " +
            "  `ps_partkey`     INT , " +
            "  `ps_suppkey`     INT , " +
            "  `ps_availqty`    INT , " +
            "  `ps_supplycost`  DECIMAL(15,2)  , " +
            "  `ps_comment`     STRING, " +
            "  `ps_dummy`       STRING) " +
            "LOCATION '/tmp/tpch_test_data/partsupp'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE  IF NOT EXISTS `%s`.`customer` (" +
            "  `c_custkey`     INT , " +
            "  `c_name`        STRING , " +
            "  `c_address`     STRING , " +
            "  `c_nationkey`   INT , " +
            "  `c_phone`       STRING , " +
            "  `c_acctbal`     DECIMAL(15,2)   , " +
            "  `c_mktsegment`  STRING , " +
            "  `c_comment`     STRING, " +
            "  `c_dummy`       STRING) " +
            "LOCATION '/tmp/tpch_test_data/customer'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS  `%s`.`orders`  ( " +
            "  `o_orderkey`       INT , " +
            "  `o_custkey`        INT , " +
            "  `o_orderstatus`    STRING , " +
            "  `o_totalprice`     DECIMAL(15,2) , " +
            "  `o_orderdate`      STRING , " +    // incorrect! TIMESTAMP should be used
            "  `o_orderpriority`  STRING , " +
            "  `o_clerk`          STRING , " +
            "  `o_shippriority`   INT, " +
            "  `o_comment`        STRING, " +
            "  `o_dummy`          STRING) " +
            "LOCATION '/tmp/tpch_test_data/orders'",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`lineitem` ( " +
            "  `l_orderkey`    INT , " +
            "  `l_partkey`     INT , " +
            "  `l_suppkey`     INT , " +
            "  `l_linenumber`  INT , " +
            "  `l_quantity`    DECIMAL(15,2) , " +
            "  `l_extendedprice`  DECIMAL(15,2) , " +
            "  `l_discount`    DECIMAL(15,2) , " +
            "  `l_tax`         DECIMAL(15,2) , " +
            "  `l_returnflag`  STRING , " +
            "  `l_linestatus`  STRING , " +
            "  `l_shipdate`    STRING , " +    // incorrect! TIMESTAMP should be used
            "  `l_commitdate`  STRING , " +    // incorrect! TIMESTAMP should be used
            "  `l_receiptdate` STRING , " +    // incorrect! TIMESTAMP should be used
            "  `l_shipinstruct` STRING , " +
            "  `l_shipmode`     STRING , " +
            "  `l_comment`      STRING, " +
            "  `l_dummy`        STRING) " +
            "LOCATION '/tmp/tpch_test_data/lineitem'",
        schema));

    return conn;
  }

  public static Connection setupMySqlForDataTypeTest(
      String connectionString, String user, String password, String schema, String table)
      throws SQLException, VerdictDBDbmsException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s`", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`%s` ("
            + "bitCol        BIT(1), "
            + "tinyintCol    TINYINT(2), "
            + "boolCol       BOOL, "
            + "smallintCol   SMALLINT(3), "
            + "mediumintCol  MEDIUMINT(4), "
            + "intCol        INT(4), "
            + "integerCol    INTEGER(4), "
            + "bigintCol     BIGINT(8), "
            + "decimalCol    DECIMAL(4,2), "
            + "decCol        DEC(4,2), "
            + "floatCol      FLOAT(4,2), "
            + "doubleCol     DOUBLE(8,2), "
            + "doubleprecisionCol DOUBLE PRECISION(8,2), "
            + "dateCol       DATE, "
            + "datetimeCol   DATETIME, "
            + "timestampCol  TIMESTAMP, "
            + "timeCol       TIME, "
            + "yearCol       YEAR(2), "
            + "yearCol2      YEAR(4), "
            + "charCol       CHAR(4), "
            + "varcharCol    VARCHAR(4), "
            + "binaryCol     BINARY(4), "
            + "varbinaryCol  VARBINARY(4), "
            + "tinyblobCol   TINYBLOB, "
            + "tinytextCol   TINYTEXT, "
            + "blobCol       BLOB(4), "
            + "textCol       TEXT(100), "
            + "medimumblobCol MEDIUMBLOB, "
            + "medimumtextCol MEDIUMTEXT, "
            + "longblobCol   LONGBLOB, "
            + "longtextCol   LONGTEXT, "
            + "enumCol       ENUM('1', '2'), "
            + "setCol        SET('1', '2'))"
        , schema, table));

    dbmsConn.execute(String.format("INSERT INTO `%s`.`%s` VALUES ( "
            + "1, 2, 1, 1, 1, 1, 1, 1, "
            + "1.0, 1.0, 1.0, 1.0, 1.0, "
            + "'2018-12-31', '2018-12-31 01:00:00', '2018-12-31 00:00:01', '10:59:59', "
            + "18, 2018, 'abc', 'abc', '10', '10', "
            + "'10', 'a', '10', 'abc', '1110', 'abc', '1110', 'abc', '1', '2')",
        schema, table));

    dbmsConn.execute(String.format("INSERT INTO `%s`.`%s` VALUES ( "
            + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        schema, table));

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

        "CREATE TABLE IF NOT EXISTS `%s`.`nation` (" +
            "  `n_nationkey`  INT, " +
            "  `n_name`       CHAR(25), " +
            "  `n_regionkey`  INT, " +
            "  `n_comment`    VARCHAR(152), " +
            "  `n_dummy`      VARCHAR(10), " +
            "  PRIMARY KEY (`n_nationkey`))",
        schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE IF NOT EXISTS `%s`.`region`  (" +
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

  public static Connection setupPostgresqlForDataTypeTest(
      String connectionString, String user, String password, String schema, String table)
      throws SQLException, VerdictDBDbmsException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = new JdbcConnection(conn, new PostgresqlSyntax());

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));
    dbmsConn.execute(String.format(
        "CREATE TABLE \"%s\".\"%s\" ("
            + "bigintCol      bigint, "
            + "bigserialCol   bigserial, "
            + "bitCol         bit(1), "
            + "varbitCol      varbit(4), "
            + "booleanCol     boolean, "
            + "boxCol         box, "
            + "byteaCol       bytea, "
            + "charCol        char(4), "
            + "varcharCol     varchar(4), "
            + "cidrCol        cidr, "
            + "circleCol      circle, "
            + "dateCol        date, "
            + "float8Col      float8, "
            + "inetCol        inet, "
            + "integerCol     integer, "
            + "jsonCol        json, "
            + "lineCol        line, "
            + "lsegCol        lseg, "
            + "macaddrCol     macaddr, "
            + "macaddr8Col    macaddr8, "
            + "moneyCol       money, "
            + "numericCol     numeric(4,2), "
            + "pathCol        path, "
            + "pointCol       point, "
            + "polygonCol     polygon, "
            + "realCol        real, "
            + "smallintCol    smallint, "
            + "smallserialCol smallserial, "
            + "serialCol      serial, "
            + "textCol        text, "
            + "timeCol        time, "
            + "timestampCol   timestamp, "
            + "uuidCol        uuid, "
            // dongyoungy: xml type is not supported because its object is not serializable
//            + "xmlCol         xml,"
            + "bitvaryCol     bit varying(1),"
            + "int8Col        int8,"
            + "boolCol        bool,"
            + "characterCol   character(4),"
            + "charactervCol  character varying(4),"
            + "intCol         int,"
            + "int4Col        int4,"
            + "doublepCol     double precision,"
            + "decimalCol     decimal(4,2),"
            + "float4Col      float,"
            + "int2Col        int2,"
            + "serial2Col     serial2,"
            + "serial4Col     serial4,"
            + "timetzCol      timetz,"
            + "timestamptzCol timestamptz,"
            + "serial8Col     serial8)"
        , schema, table));

    dbmsConn.execute(String.format("INSERT INTO \"%s\".\"%s\" VALUES ( "
            + "1, 1, '1', '1011', true, '((1,1), (2,2))', '1', '1234', '1234', "
            + "'10', '((1,1),2)', '2018-12-31', 1.0, '88.99.0.0/16', 1, "
            + "'{\"2\":1}', '{1,2,3}', '((1,1),(2,2))', "
            + "'08002b:010203', '08002b:0102030405', '12.34', 1.0, '((1,1))', '(1,1)', "
            + "'((1,1))', 1.0, 1, 1, 1, '1110', '2018-12-31 00:00:01', '2018-12-31 00:00:01', "
            + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'," +
//            "'<foo>bar</foo>'," +
            " '1', 1, true, '1234', '1234', 1, 1, 1.0, 1.0, 1.0"
            + ", 1, 1, 1, '2018-12-31 00:00:01', '2018-12-31 00:00:01', 1)",
        schema, table));
    dbmsConn.execute(String.format("INSERT INTO \"%s\".\"%s\" VALUES ( "
            + "NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, NULL, NULL," +
//            "NULL," +
            "NULL, NULL, NULL, NULL, NULL, NULL, "
            + "NULL, NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, 1)",
        schema, table));

    return conn;
  }

  public static Connection setupRedshiftForDataTypeTest(
      String connectionString, String user, String password, String schema, String table)
      throws SQLException, VerdictDBDbmsException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = new JdbcConnection(conn, new RedshiftSyntax());

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));
    // These types are gathered from (Jul 2018):
    // https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
    dbmsConn.execute(String.format(
        "CREATE TABLE \"%s\".\"%s\" ("
            + "smallintCol    smallint, "
            + "int2Col   int2, "
            + "integerCol         integer, "
            + "intCol      int, "
            + "int4Col     int4, "
            + "bigintCol         bigint, "
            + "decimalCol       decimal(5,2), "
            + "numericCol        numeric(5,2), "
            + "realCol     real, "
            + "float4Col        float4, "
            + "doublePrecCol      double precision, "
            + "float8Col        float8, "
            + "floatCol      float, "
            + "booleanCol        boolean, "
            + "boolCol     bool, "
            + "charCol        char(10), "
            + "characterCol   character(10), "
            + "ncharCol        nchar(10), "
            + "bpcharCol     bpchar, "
            + "varcharCol    varchar(10), "
            + "charactervarCol character varying(10), "
            + "nvarcharCol     nvarchar(10), "
            + "textCol        text, "
            + "dateCol       date,"
            + "timestampCol     timestamp, "
            + "timestampwtzCol timestamp without time zone) "
        // dongyoungy: below two types are problematic as redshift uses a custom object
        // and we currently do not include such object. The error msg is
        // java.lang.ClassNotFoundException: com.amazon.redshift.api.PGTimestamp
//            + "timestamptzCol    timestamptz) "
//            + "timestamptzCol2 timestamp with time zone)"
        , schema, table));

    List<String> insertDataList = new ArrayList<>();
    insertDataList.add("1"); // smallint
    insertDataList.add("2"); // int2
    insertDataList.add("3"); // integer
    insertDataList.add("4"); // int
    insertDataList.add("5"); // int4
    insertDataList.add("6"); // bigint
    insertDataList.add("123.45"); // decimal
    insertDataList.add("-123.45"); // numeric
    insertDataList.add("1000.001"); // real
    insertDataList.add("1000.001"); // float4
    insertDataList.add("1000.001"); // double precision
    insertDataList.add("1000.001"); // float8
    insertDataList.add("1000.001"); // float
    insertDataList.add("true"); // boolean
    insertDataList.add("false"); // bool
    insertDataList.add("'john'"); // char
    insertDataList.add("'kim'"); // character
    insertDataList.add("'michael'"); // nchar
    insertDataList.add("'jackson'"); // bpchar
    insertDataList.add("'yo'"); // varchar
    insertDataList.add("'hey'"); // character varying
    insertDataList.add("'sup'"); // nvarchar
    insertDataList.add("'sometext'"); // text
    insertDataList.add("'2018-12-31'"); // date
    insertDataList.add("'2018-12-31 11:22:33'"); // timestamp
    insertDataList.add("'2018-12-31 11:22:33'"); // timestamp without time zone
//    insertDataList.add("'2018-12-31 11:22:33'"); // timestamptz
//    insertDataList.add("'2018-12-31 11:22:33'"); // timestamp with time zone

    dbmsConn.execute(String.format("INSERT INTO \"%s\".\"%s\" VALUES (%s)",
        schema, table, Joiner.on(",").join(insertDataList)));

    List<String> insertNullDataList = new ArrayList<>();
    insertNullDataList.add("NULL"); // smallint
    insertNullDataList.add("NULL"); // int2
    insertNullDataList.add("NULL"); // integer
    insertNullDataList.add("NULL"); // int
    insertNullDataList.add("NULL"); // int4
    insertNullDataList.add("NULL"); // bigint
    insertNullDataList.add("NULL"); // decimal
    insertNullDataList.add("NULL"); // numeric
    insertNullDataList.add("NULL"); // real
    insertNullDataList.add("NULL"); // float4
    insertNullDataList.add("NULL"); // double precision
    insertNullDataList.add("NULL"); // float8
    insertNullDataList.add("NULL"); // float
    insertNullDataList.add("NULL"); // boolean
    insertNullDataList.add("NULL"); // bool
    insertNullDataList.add("NULL"); // char
    insertNullDataList.add("NULL"); // character
    insertNullDataList.add("NULL"); // nchar
    insertNullDataList.add("NULL"); // bpchar
    insertNullDataList.add("NULL"); // varchar
    insertNullDataList.add("NULL"); // character varying
    insertNullDataList.add("NULL"); // nvarchar
    insertNullDataList.add("NULL"); // text
    insertNullDataList.add("NULL"); // date
    insertNullDataList.add("NULL"); // timestamp
    insertNullDataList.add("NULL"); // timestamp without time zone
//    insertNullDataList.add("NULL"); // timestamptz
//    insertNullDataList.add("NULL"); // timestamp with time zone

    dbmsConn.execute(String.format("INSERT INTO \"%s\".\"%s\" VALUES (%s)",
        schema, table, Joiner.on(",").join(insertNullDataList)));

    return conn;
  }

  public static Connection setupImpalaForDataTypeTest(
      String connectionString, String user, String password, String schema, String table)
      throws SQLException, VerdictDBDbmsException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = new JdbcConnection(conn, new ImpalaSyntax());

    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS `%s`.`%s`", schema, table));
    // These types are gathered from (Jul 2018):
    // https://www.cloudera.com/documentation/enterprise/latest/topics/impala_datatypes.html#datatypes
    dbmsConn.execute(String.format(
        "CREATE TABLE `%s`.`%s` ("
            + "bigintCol    bigint, "
            + "booleanCol   boolean, "
            + "charCol         char(10), "
            + "decimalCol      decimal(5,2), "
            + "doubleCol     double, "
            + "floatCol         float, "
            + "realCol       real, "
            + "smallintCol        smallint, "
            + "stringCol     string, "
            + "timestampCol        timestamp, "
            + "tinyintCol      tinyint) "
        // dongyoungy: current version of impala we testing does not seem to support varchar
//            + "varcharCol        varchar(10))"
        , schema, table));
//    dbmsConn.execute("INVALIDATE METADATA");
//    dbmsConn.execute(String.format("REFRESH %s.%s", schema, table));

    List<String> insertDataList = new ArrayList<>();
    insertDataList.add("6"); // bigint
    insertDataList.add("true"); // boolean
    insertDataList.add("cast('john' as char(10))"); // char
    insertDataList.add("123.45"); // decimal
    insertDataList.add("1000.001"); // double
    insertDataList.add("1000.001"); // float
    insertDataList.add("1000.001"); // real
    insertDataList.add("1"); // smallint
    insertDataList.add("'michael'"); // string
    insertDataList.add("now()"); // timestamp
    insertDataList.add("2"); // tinyint

    dbmsConn.execute(String.format("INSERT INTO `%s`.`%s` VALUES (%s)",
        schema, table, Joiner.on(",").join(insertDataList)));

    List<String> insertNullDataList = new ArrayList<>();

    insertNullDataList.add("NULL"); // bigint
    insertNullDataList.add("NULL"); // boolean
    insertNullDataList.add("NULL"); // char
    insertNullDataList.add("NULL"); // decimal
    insertNullDataList.add("NULL"); // double
    insertNullDataList.add("NULL"); // float
    insertNullDataList.add("NULL"); // real
    insertNullDataList.add("NULL"); // smallint
    insertNullDataList.add("NULL"); // string
    insertNullDataList.add("NULL"); // timestamp
    insertNullDataList.add("NULL"); // tinyint

    dbmsConn.execute(String.format("INSERT INTO `%s`.`%s` VALUES (%s)",
        schema, table, Joiner.on(",").join(insertNullDataList)));

    return conn;
  }

}
