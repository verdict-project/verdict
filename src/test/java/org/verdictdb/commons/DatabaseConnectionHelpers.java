package org.verdictdb.commons;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.SparkSession;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.ImpalaSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.PrestoHiveSyntax;
import org.verdictdb.sqlsyntax.RedshiftSyntax;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class DatabaseConnectionHelpers {

  // Default connection variables for databases used in unit tests
  public static final String MYSQL_HOST;

  public static final String MYSQL_DATABASE =
      "verdictdb_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  public static final String MYSQL_USER = "root";

  public static final String MYSQL_PASSWORD = "";

  public static final String IMPALA_HOST;

  public static final String IMPALA_DATABASE =
      "verdictdb_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  public static final String IMPALA_USER = "";

  public static final String IMPALA_PASSWORD = "";

  public static final String REDSHIFT_HOST;

  public static final String REDSHIFT_DATABASE = "dev";

  public static final String REDSHIFT_SCHEMA =
      "verdictdb_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  public static final String REDSHIFT_USER;

  public static final String REDSHIFT_PASSWORD;

  public static final String POSTGRES_HOST;

  public static final String POSTGRES_DATABASE = "test";

  public static final String POSTGRES_USER = "postgres";

  public static final String POSTGRES_PASSWORD = "";

  public static final String POSTGRES_SCHEMA =
      "verdictdb_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  static {
    IMPALA_HOST = System.getenv("VERDICTDB_TEST_IMPALA_HOST");
  }

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      POSTGRES_HOST = "postgres";
    } else {
      POSTGRES_HOST = "localhost";
    }
  }

  static {
    REDSHIFT_HOST = System.getenv("VERDICTDB_TEST_REDSHIFT_ENDPOINT");
    REDSHIFT_USER = System.getenv("VERDICTDB_TEST_REDSHIFT_USER");
    REDSHIFT_PASSWORD = System.getenv("VERDICTDB_TEST_REDSHIFT_PASSWORD");
  }

  public static final String COMMON_TABLE_NAME = "mytable";
  public static final String COMMON_SCHEMA_NAME =
      "verdictdb_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
  public static final String TEMPLATE_SCHEMA_NAME = "VERDICTDB_TEST_DBNAME";

  public static SparkSession setupSpark(String appname, String schema) {
    SparkSession spark =
        SparkSession.builder().appName(appname).master("local").enableHiveSupport().getOrCreate();
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
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`nation` ("
                + "  `n_nationkey`  INT, "
                + "  `n_name`       CHAR(25), "
                + "  `n_regionkey`  INT, "
                + "  `n_comment`    VARCHAR(152), "
                + "  `n_dummy`      VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS TEXTFILE "
                + "LOCATION '%s/nation'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`region` ("
                + "  `r_regionkey`  INT, "
                + "  `r_name`       CHAR(25), "
                + "  `r_comment`    VARCHAR(152), "
                + "  `r_dummy`      VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS TEXTFILE "
                + "LOCATION '%s/region'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`part`  ("
                + "  `p_partkey`     INT, "
                + "  `p_name`        VARCHAR(55), "
                + "  `p_mfgr`        CHAR(25), "
                + "  `p_brand`       CHAR(10), "
                + "  `p_type`        VARCHAR(25), "
                + "  `p_size`        INT, "
                + "  `p_container`   CHAR(10), "
                + "  `p_retailprice` DECIMAL(15,2) , "
                + "  `p_comment`     VARCHAR(23) , "
                + "  `p_dummy`       VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS textfile "
                + "LOCATION '%s/part'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`supplier` ( "
                + "  `s_suppkey`     INT , "
                + "  `s_name`        CHAR(25) , "
                + "  `s_address`     VARCHAR(40) , "
                + "  `s_nationkey`   INT , "
                + "  `s_phone`       CHAR(15) , "
                + "  `s_acctbal`     DECIMAL(15,2) , "
                + "  `s_comment`     VARCHAR(101), "
                + "  `s_dummy`       VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS textfile "
                + "LOCATION '%s/supplier'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`partsupp` ( "
                + "  `ps_partkey`     INT , "
                + "  `ps_suppkey`     INT , "
                + "  `ps_availqty`    INT , "
                + "  `ps_supplycost`  DECIMAL(15,2)  , "
                + "  `ps_comment`     VARCHAR(199), "
                + "  `ps_dummy`       VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS textfile "
                + "LOCATION '%s/partsupp'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`customer` ("
                + "  `c_custkey`     INT , "
                + "  `c_name`        VARCHAR(25) , "
                + "  `c_address`     VARCHAR(40) , "
                + "  `c_nationkey`   INT , "
                + "  `c_phone`       CHAR(15) , "
                + "  `c_acctbal`     DECIMAL(15,2)   , "
                + "  `c_mktsegment`  CHAR(10) , "
                + "  `c_comment`     VARCHAR(117), "
                + "  `c_dummy`       VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS textfile "
                + "LOCATION '%s/customer'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS  `%s`.`orders`  ( "
                + "  `o_orderkey`       INT , "
                + "  `o_custkey`        INT , "
                + "  `o_orderstatus`    CHAR(1) , "
                + "  `o_totalprice`     DECIMAL(15,2) , "
                + "  `o_orderdate`      DATE , "
                + "  `o_orderpriority`  CHAR(15) , "
                + "  `o_clerk`          CHAR(15) , "
                + "  `o_shippriority`   INT , "
                + "  `o_comment`        VARCHAR(79), "
                + "  `o_dummy`          VARCHAR(10)) "
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS textfile "
                + "LOCATION '%s/orders'",
            schema, datafilePath));
    spark.sql(
        String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS `%s`.`lineitem` ("
                + "  `l_orderkey`       INT , "
                + "  `l_partkey`        INT , "
                + "  `l_suppkey`        INT , "
                + "  `l_linenumber`     INT , "
                + "  `l_quantity`       DECIMAL(15,2) , "
                + "  `l_extendedprice`  DECIMAL(15,2) , "
                + "  `l_discount`       DECIMAL(15,2) , "
                + "  `l_tax`            DECIMAL(15,2) , "
                + "  `l_returnflag`     CHAR(1) , "
                + "  `l_linestatus`     CHAR(1) , "
                + "  `l_shipdate`       DATE , "
                + "  `l_commitdate`     DATE , "
                + "  `l_receiptdate`    DATE , "
                + "  `l_shipinstruct`   CHAR(25) , "
                + "  `l_shipmode`       CHAR(10) , "
                + "  `l_comment`        VARCHAR(44), "
                + "  `l_dummy`          VARCHAR(10))"
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
                + "STORED AS textfile "
                + "LOCATION '%s/lineitem'",
            schema, datafilePath));

    return spark;
  }

  public static Connection setupImpala(
      String connectionString, String user, String password, String schema)
      throws SQLException, VerdictDBDbmsException, IOException {

    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s` CASCADE", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));

    // Create tables
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`nation` ("
                + "  `n_nationkey`  INT, "
                + "  `n_name`       STRING, "
                + "  `n_regionkey`  INT, "
                + "  `n_comment`    STRING, "
                + "  `n_dummy`      STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'",
            //                + "LOCATION '/tmp/tpch_test_data/nation'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`region`  ("
                + "  `r_regionkey`  INT, "
                + "  `r_name`       STRING, "
                + "  `r_comment`    STRING, "
                + "  `r_dummy`      STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'",
            //                + "LOCATION '/tmp/tpch_test_data/region'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`part`  ( "
                + "  `p_partkey`     INT, "
                + "  `p_name`        STRING, "
                + "  `p_mfgr`        STRING, "
                + "  `p_brand`       STRING, "
                + "  `p_type`        STRING, "
                + "  `p_size`        INT, "
                + "  `p_container`   STRING, "
                + "  `p_retailprice` DECIMAL(15,2) , "
                + "  `p_comment`     STRING, "
                + "  `p_dummy`       STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            //                + "LOCATION '/tmp/tpch_test_data/part'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`supplier` ( "
                + "  `s_suppkey`     INT , "
                + "  `s_name`        STRING , "
                + "  `s_address`     STRING, "
                + "  `s_nationkey`   INT , "
                + "  `s_phone`       STRING , "
                + "  `s_acctbal`     DECIMAL(15,2) , "
                + "  `s_comment`     STRING, "
                + "  `s_dummy`       STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            //                + "LOCATION '/tmp/tpch_test_data/supplier'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`partsupp` ( "
                + "  `ps_partkey`     INT , "
                + "  `ps_suppkey`     INT , "
                + "  `ps_availqty`    INT , "
                + "  `ps_supplycost`  DECIMAL(15,2)  , "
                + "  `ps_comment`     STRING, "
                + "  `ps_dummy`       STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            //                + "LOCATION '/tmp/tpch_test_data/partsupp'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS `%s`.`customer` ("
                + "  `c_custkey`     INT , "
                + "  `c_name`        STRING , "
                + "  `c_address`     STRING , "
                + "  `c_nationkey`   INT , "
                + "  `c_phone`       STRING , "
                + "  `c_acctbal`     DECIMAL(15,2)   , "
                + "  `c_mktsegment`  STRING , "
                + "  `c_comment`     STRING, "
                + "  `c_dummy`       STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            //                + "LOCATION '/tmp/tpch_test_data/customer'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS  `%s`.`orders`  ( "
                + "  `o_orderkey`       INT , "
                + "  `o_custkey`        INT , "
                + "  `o_orderstatus`    STRING , "
                + "  `o_totalprice`     DECIMAL(15,2) , "
                + "  `o_orderdate`      TIMESTAMP , "
                + "  `o_orderpriority`  STRING , "
                + "  `o_clerk`          STRING , "
                + "  `o_shippriority`   INT, "
                + "  `o_comment`        STRING, "
                + "  `o_dummy`          STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            //                + "LOCATION '/tmp/tpch_test_data/orders'",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`lineitem` ( "
                + "  `l_orderkey`    INT , "
                + "  `l_partkey`     INT , "
                + "  `l_suppkey`     INT , "
                + "  `l_linenumber`  INT , "
                + "  `l_quantity`    DECIMAL(15,2) , "
                + "  `l_extendedprice`  DECIMAL(15,2) , "
                + "  `l_discount`    DECIMAL(15,2) , "
                + "  `l_tax`         DECIMAL(15,2) , "
                + "  `l_returnflag`  STRING , "
                + "  `l_linestatus`  STRING , "
                + "  `l_shipdate`    TIMESTAMP , "
                + "  `l_commitdate`  TIMESTAMP , "
                + "  `l_receiptdate` TIMESTAMP , "
                + "  `l_shipinstruct` STRING , "
                + "  `l_shipmode`     STRING , "
                + "  `l_comment`      STRING, "
                + "  `l_dummy`        STRING) ",
            //                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'"
            //                + "LOCATION '/tmp/tpch_test_data/lineitem'",
            schema));

    // load data use insert
    loadImpalaData(schema, "nation", conn);
    loadImpalaData(schema, "region", conn);
    loadImpalaData(schema, "part", conn);
    loadImpalaData(schema, "supplier", conn);
    loadImpalaData(schema, "customer", conn);
    loadImpalaData(schema, "partsupp", conn);
    loadImpalaData(schema, "orders", conn);
    loadImpalaData(schema, "lineitem", conn);
    return conn;
  }

  static String getQuoted(String value) {
    return "'" + value + "'";
  }

  static void loadRedshiftData(String schema, String table, DbmsConnection dbmsConn)
      throws IOException, VerdictDBDbmsException {
    String concat = "";
    File file =
        new File(String.format("src/test/resources/tpch_test_data/%s/%s.tbl", table, table));
    DbmsQueryResult columnMeta =
        dbmsConn.execute(
            String.format(
                "select data_type, ordinal_position from INFORMATION_SCHEMA.COLUMNS where table_name='%s' and table_schema='%s'",
                table, schema));
    List<Boolean> quotedNeeded = new ArrayList<>();
    for (int i = 0; i < columnMeta.getRowCount(); i++) {
      quotedNeeded.add(true);
    }
    while (columnMeta.next()) {
      String columnType = columnMeta.getString(0);
      int columnIndex = columnMeta.getInt(1);
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
    dbmsConn.execute(String.format("insert into \"%s\".\"%s\" values %s", schema, table, concat));
  }

  public static Connection setupRedshift(
      String connectionString, String user, String password, String schema)
      throws VerdictDBDbmsException, SQLException, IOException {

    Connection conn = DriverManager.getConnection(connectionString, user, password);
    JdbcConnection dbmsConn = new JdbcConnection(conn, new RedshiftSyntax());
    //    dbmsConn.setOutputDebugMessage(true);

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));

    // Create tables
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"nation\" ("
                + "  \"n_nationkey\"  INT, "
                + "  \"n_name\"       CHAR(25), "
                + "  \"n_regionkey\"  INT, "
                + "  \"n_comment\"    VARCHAR(152), "
                + "  \"n_dummy\"      VARCHAR(10), "
                + "  PRIMARY KEY (\"n_nationkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"region\"  ("
                + "  \"r_regionkey\"  INT, "
                + "  \"r_name\"       CHAR(25), "
                + "  \"r_comment\"    VARCHAR(152), "
                + "  \"r_dummy\"      VARCHAR(10), "
                + "  PRIMARY KEY (\"r_regionkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"part\"  ( \"p_partkey\"     INT, "
                + "  \"p_name\"        VARCHAR(55), "
                + "  \"p_mfgr\"        CHAR(25), "
                + "  \"p_brand\"       CHAR(10), "
                + "  \"p_type\"        VARCHAR(25), "
                + "  \"p_size\"        INT, "
                + "  \"p_container\"   CHAR(10), "
                + "  \"p_retailprice\" DECIMAL(15,2) , "
                + "  \"p_comment\"     VARCHAR(23) , "
                + "  \"p_dummy\"       VARCHAR(10), "
                + "  PRIMARY KEY (\"p_partkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"supplier\" ( "
                + "  \"s_suppkey\"     INT , "
                + "  \"s_name\"        CHAR(25) , "
                + "  \"s_address\"     VARCHAR(40) , "
                + "  \"s_nationkey\"   INT , "
                + "  \"s_phone\"       CHAR(15) , "
                + "  \"s_acctbal\"     DECIMAL(15,2) , "
                + "  \"s_comment\"     VARCHAR(101), "
                + "  \"s_dummy\" varchar(10), "
                + "  PRIMARY KEY (\"s_suppkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"partsupp\" ( "
                + "  \"ps_partkey\"     INT , "
                + "  \"ps_suppkey\"     INT , "
                + "  \"ps_availqty\"    INT , "
                + "  \"ps_supplycost\"  DECIMAL(15,2)  , "
                + "  \"ps_comment\"     VARCHAR(199), "
                + "  \"ps_dummy\"       VARCHAR(10), "
                + "  PRIMARY KEY (\"ps_partkey\", \"ps_suppkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"customer\" ("
                + "  \"c_custkey\"     INT , "
                + "  \"c_name\"        VARCHAR(25) , "
                + "  \"c_address\"     VARCHAR(40) , "
                + "  \"c_nationkey\"   INT , "
                + "  \"c_phone\"       CHAR(15) , "
                + "  \"c_acctbal\"     DECIMAL(15,2)   , "
                + "  \"c_mktsegment\"  CHAR(10) , "
                + "  \"c_comment\"     VARCHAR(117), "
                + "  \"c_dummy\"       VARCHAR(10), "
                + "  PRIMARY KEY (\"c_custkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS  \"%s\".\"orders\"  ( "
                + "  \"o_orderkey\"       INT , "
                + "  \"o_custkey\"        INT , "
                + "  \"o_orderstatus\"    CHAR(1) , "
                + "  \"o_totalprice\"     DECIMAL(15,2) , "
                + "  \"o_orderdate\"      DATE , "
                + "  \"o_orderpriority\"  CHAR(15) , "
                + "  \"o_clerk\"          CHAR(15) , "
                + "  \"o_shippriority\"   INT , "
                + "  \"o_comment\"        VARCHAR(79), "
                + "  \"o_dummy\" varchar(10), "
                + "  PRIMARY KEY (\"o_orderkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS \"%s\".\"lineitem\" ("
                + "  \"l_orderkey\"    INT , "
                + "  \"l_partkey\"     INT , "
                + "  \"l_suppkey\"     INT , "
                + "  \"l_linenumber\"  INT , "
                + "  \"l_quantity\"    DECIMAL(15,2) , "
                + "  \"l_extendedprice\"  DECIMAL(15,2) , "
                + "  \"l_discount\"    DECIMAL(15,2) , "
                + "  \"l_tax\"         DECIMAL(15,2) , "
                + "  \"l_returnflag\"  CHAR(1) , "
                + "  \"l_linestatus\"  CHAR(1) , "
                + "  \"l_shipdate\"    DATE , "
                + "  \"l_commitdate\"  DATE , "
                + "  \"l_receiptdate\" DATE , "
                + "  \"l_shipinstruct\" CHAR(25) , "
                + "  \"l_shipmode\"     CHAR(10) , "
                + "  \"l_comment\"      VARCHAR(44), "
                + "  \"l_dummy\" varchar(10))",
            schema));

    // load data use insert
    loadRedshiftData(schema, "nation", dbmsConn);
    loadRedshiftData(schema, "region", dbmsConn);
    loadRedshiftData(schema, "part", dbmsConn);
    loadRedshiftData(schema, "supplier", dbmsConn);
    loadRedshiftData(schema, "customer", dbmsConn);
    loadRedshiftData(schema, "partsupp", dbmsConn);
    loadRedshiftData(schema, "orders", dbmsConn);
    loadRedshiftData(schema, "lineitem", dbmsConn);
    return conn;
  }

  private static void loadImpalaData(String schema, String table, Connection conn)
      throws IOException, SQLException {
    File file =
        new File(String.format("src/test/resources/tpch_test_data/%s/%s.tbl", table, table));
    ResultSet rs =
        conn.createStatement().executeQuery(String.format("DESCRIBE %s.%s", schema, table));
    List<Boolean> quotedNeeded = new ArrayList<>();
    List<Boolean> isDate = new ArrayList<>();
    while (rs.next()) {
      String columnType = rs.getString(2).toLowerCase();
      if (columnType.equals("integer")
          || columnType.equals("numeric")
          || columnType.contains("double")
          || columnType.contains("decimal")
          || columnType.contains("bigint")
          || columnType.equals("int")) {
        quotedNeeded.add(false);
      } else quotedNeeded.add(true);
      if (columnType.contains("date")) {
        isDate.add(true);
      } else isDate.add(false);
    }

    StringBuilder concat = new StringBuilder();
    String content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      StringBuilder rowBuilder = new StringBuilder();
      for (int i = 0; i < values.length - 1; i++) {
        if (quotedNeeded.get(i)) {
          rowBuilder.append(isDate.get(i) ? "date " : "").append(getQuoted(values[i])).append(",");
        } else {
          rowBuilder.append(values[i]).append(",");
        }
      }
      for (int i = 0; i < quotedNeeded.size() - values.length + 1; ++i) {
        if (i < quotedNeeded.size() - values.length) {
          rowBuilder.append("'',");
        } else {
          rowBuilder.append("''");
        }
      }
      row = rowBuilder.toString();
      if (concat.toString().equals("")) {
        concat.append("(").append(row).append(")");
      } else concat.append(",").append("(").append(row).append(")");
    }
    conn.createStatement()
        .execute(String.format("insert into %s.%s values %s", schema, table, concat.toString()));
  }

  private static void loadPrestoData(String schema, String table, Connection conn)
      throws IOException, SQLException {
    String concat = "";
    File file =
        new File(String.format("src/test/resources/tpch_test_data/%s/%s.tbl", table, table));
    ResultSet rs =
        conn.createStatement().executeQuery(String.format("DESCRIBE %s.%s", schema, table));
    List<Boolean> quotedNeeded = new ArrayList<>();
    List<Boolean> isDate = new ArrayList<>();
    while (rs.next()) {
      String columnType = rs.getString(2).toLowerCase();
      if (columnType.equals("integer")
          || columnType.equals("numeric")
          || columnType.contains("double")
          || columnType.contains("decimal")
          || columnType.contains("bigint")) {
        quotedNeeded.add(false);
      } else quotedNeeded.add(true);
      if (columnType.contains("date")) {
        isDate.add(true);
      } else isDate.add(false);
    }

    String content = Files.toString(file, Charsets.UTF_8);
    for (String row : content.split("\n")) {
      String[] values = row.split("\\|");
      row = "";
      for (int i = 0; i < values.length - 1; i++) {
        if (quotedNeeded.get(i)) {
          row = row + (isDate.get(i) ? "date " : "") + getQuoted(values[i]) + ",";
        } else {
          row = row + values[i] + ",";
        }
      }
      for (int i = 0; i < quotedNeeded.size() - values.length + 1; ++i) {
        if (i < quotedNeeded.size() - values.length) {
          row = row + "'',";
        } else {
          row = row + "''";
        }
      }
      if (concat.equals("")) {
        concat = concat + "(" + row + ")";
      } else concat = concat + "," + "(" + row + ")";
    }
    conn.createStatement()
        .execute(String.format("insert into \"%s\".\"%s\" values %s", schema, table, concat));
  }

  public static Connection setupPresto(
      String connectionString, String user, String password, String schema)
      throws SQLException, IOException {

    Connection conn = DriverManager.getConnection(connectionString, user, password);

    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));
    ResultSet rs =
        conn.createStatement().executeQuery(String.format("SHOW TABLES IN \"%s\"", schema));
    while (rs.next()) {
      conn.createStatement().execute(String.format("DROP TABLE IF EXISTS %s", rs.getString(1)));
    }

    // Create tables
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"nation\" ("
                + "  \"n_nationkey\"  INT, "
                + "  \"n_name\"       CHAR(25), "
                + "  \"n_regionkey\"  INT, "
                + "  \"n_comment\"    VARCHAR(152), "
                + "  \"n_dummy\"      VARCHAR(10)) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"region\"  ("
                + "  \"r_regionkey\"  INT, "
                + "  \"r_name\"       CHAR(25), "
                + "  \"r_comment\"    VARCHAR(152), "
                + "  \"r_dummy\"      VARCHAR(10)) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"part\"  ( \"p_partkey\"     INT, "
                + "  \"p_name\"        VARCHAR(55), "
                + "  \"p_mfgr\"        CHAR(25), "
                + "  \"p_brand\"       CHAR(10), "
                + "  \"p_type\"        VARCHAR(25), "
                + "  \"p_size\"        INT, "
                + "  \"p_container\"   CHAR(10), "
                + "  \"p_retailprice\" DECIMAL(15,2) , "
                + "  \"p_comment\"     VARCHAR(23) , "
                + "  \"p_dummy\"       VARCHAR(10)) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"supplier\" ( "
                + "  \"s_suppkey\"     INT , "
                + "  \"s_name\"        CHAR(25) , "
                + "  \"s_address\"     VARCHAR(40) , "
                + "  \"s_nationkey\"   INT , "
                + "  \"s_phone\"       CHAR(15) , "
                + "  \"s_acctbal\"     DECIMAL(15,2) , "
                + "  \"s_comment\"     VARCHAR(101), "
                + "  \"s_dummy\" varchar(10)) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"partsupp\" ( "
                + "  \"ps_partkey\"     INT , "
                + "  \"ps_suppkey\"     INT , "
                + "  \"ps_availqty\"    INT , "
                + "  \"ps_supplycost\"  DECIMAL(15,2)  , "
                + "  \"ps_comment\"     VARCHAR(199), "
                + "  \"ps_dummy\"       VARCHAR(10)) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"customer\" ("
                + "  \"c_custkey\"     INT , "
                + "  \"c_name\"        VARCHAR(25) , "
                + "  \"c_address\"     VARCHAR(40) , "
                + "  \"c_nationkey\"   INT , "
                + "  \"c_phone\"       CHAR(15) , "
                + "  \"c_acctbal\"     DECIMAL(15,2)   , "
                + "  \"c_mktsegment\"  CHAR(10) , "
                + "  \"c_comment\"     VARCHAR(117), "
                + "  \"c_dummy\"       VARCHAR(10)) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS  \"%s\".\"orders\"  ( "
                + "  \"o_orderkey\"       INT , "
                + "  \"o_custkey\"        INT , "
                + "  \"o_orderstatus\"    CHAR(1) , "
                + "  \"o_totalprice\"     DECIMAL(15,2) , "
                + "  \"o_orderdate\"      DATE , "
                + "  \"o_orderpriority\"  CHAR(15) , "
                + "  \"o_clerk\"          CHAR(15) , "
                + "  \"o_shippriority\"   INT , "
                + "  \"o_comment\"        VARCHAR(79), "
                + "  \"o_dummy\" varchar(10)) WITH (partitioned_by = ARRAY['o_dummy']) ",
            schema));
    stmt.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS \"%s\".\"lineitem\" ("
                + "  \"l_orderkey\"    INT , "
                + "  \"l_partkey\"     INT , "
                + "  \"l_suppkey\"     INT , "
                + "  \"l_linenumber\"  INT , "
                + "  \"l_quantity\"    DECIMAL(15,2) , "
                + "  \"l_extendedprice\"  DECIMAL(15,2) , "
                + "  \"l_discount\"    DECIMAL(15,2) , "
                + "  \"l_tax\"         DECIMAL(15,2) , "
                + "  \"l_returnflag\"  CHAR(1) , "
                + "  \"l_linestatus\"  CHAR(1) , "
                + "  \"l_shipdate\"    DATE , "
                + "  \"l_commitdate\"  DATE , "
                + "  \"l_receiptdate\" DATE , "
                + "  \"l_shipinstruct\" CHAR(25) , "
                + "  \"l_shipmode\"     CHAR(10) , "
                + "  \"l_comment\"      VARCHAR(44), "
                + "  \"l_dummy\" varchar(10))",
            schema));

    // load data use insert
    loadPrestoData(schema, "nation", conn);
    loadPrestoData(schema, "region", conn);
    loadPrestoData(schema, "part", conn);
    loadPrestoData(schema, "supplier", conn);
    loadPrestoData(schema, "customer", conn);
    loadPrestoData(schema, "partsupp", conn);
    loadPrestoData(schema, "orders", conn);
    loadPrestoData(schema, "lineitem", conn);
    return conn;
  }

  public static Connection setupMySqlForDataTypeTest(
      String connectionString, String user, String password, String schema, String table)
      throws SQLException, VerdictDBDbmsException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = JdbcConnection.create(conn);

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS `%s`", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", schema));
    dbmsConn.execute(
        String.format(
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
                + "setCol        SET('1', '2'))",
            schema, table));

    dbmsConn.execute(
        String.format(
            "INSERT INTO `%s`.`%s` VALUES ( "
                + "1, 2, 1, 1, 1, 1, 1, 1, "
                + "1.0, 1.0, 1.0, 1.0, 1.0, "
                + "'2018-12-31', '2018-12-31 01:00:00', '2018-12-31 00:00:01', '10:59:59', "
                + "18, 2018, 'abc', 'abc', '10', '10', "
                + "'10', 'a', '10', 'abc', '1110', 'abc', '1110', 'abc', '1', '2')",
            schema, table));

    dbmsConn.execute(
        String.format(
            "INSERT INTO `%s`.`%s` VALUES ( "
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
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`nation` ("
                + "  `n_nationkey`  INT, "
                + "  `n_name`       CHAR(25), "
                + "  `n_regionkey`  INT, "
                + "  `n_comment`    VARCHAR(152), "
                + "  `n_dummy`      VARCHAR(10), "
                + "  PRIMARY KEY (`n_nationkey`))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS `%s`.`region`  ("
                + "  `r_regionkey`  INT, "
                + "  `r_name`       CHAR(25), "
                + "  `r_comment`    VARCHAR(152), "
                + "  `r_dummy`      VARCHAR(10), "
                + "  PRIMARY KEY (`r_regionkey`))",
            schema));
    dbmsConn.execute(
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
    dbmsConn.execute(
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
    dbmsConn.execute(
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
    dbmsConn.execute(
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
    dbmsConn.execute(
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
    dbmsConn.execute(
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
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/region/region.tbl' "
                + "INTO TABLE `%s`.`region` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/nation/nation.tbl' "
                + "INTO TABLE `%s`.`nation` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/supplier/supplier.tbl' "
                + "INTO TABLE `%s`.`supplier` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/customer/customer.tbl' "
                + "INTO TABLE `%s`.`customer` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/part/part.tbl' "
                + "INTO TABLE `%s`.`part` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/partsupp/partsupp.tbl' "
                + "INTO TABLE `%s`.`partsupp` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/lineitem/lineitem.tbl' "
                + "INTO TABLE `%s`.`lineitem` FIELDS TERMINATED BY '|'",
            schema));
    dbmsConn.execute(
        String.format(
            "LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/orders/orders.tbl' "
                + "INTO TABLE `%s`.`orders` FIELDS TERMINATED BY '|'",
            schema));

    return conn;
  }

  public static Connection setupPostgresql(
      String connectionString, String user, String password, String schema)
      throws VerdictDBDbmsException, SQLException, IOException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = new JdbcConnection(conn, new PostgresqlSyntax());

    dbmsConn.execute(String.format("DROP SCHEMA IF EXISTS \"%s\" CASCADE", schema));
    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));

    // Create tables
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"nation\" ("
                + "  \"n_nationkey\"  INT, "
                + "  \"n_name\"       CHAR(25), "
                + "  \"n_regionkey\"  INT, "
                + "  \"n_comment\"    VARCHAR(152), "
                + "  \"n_dummy\"      VARCHAR(10), "
                + "  PRIMARY KEY (\"n_nationkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"region\"  ("
                + "  \"r_regionkey\"  INT, "
                + "  \"r_name\"       CHAR(25), "
                + "  \"r_comment\"    VARCHAR(152), "
                + "  \"r_dummy\"      VARCHAR(10), "
                + "  PRIMARY KEY (\"r_regionkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"part\"  ( \"p_partkey\"     INT, "
                + "  \"p_name\"        VARCHAR(55), "
                + "  \"p_mfgr\"        CHAR(25), "
                + "  \"p_brand\"       CHAR(10), "
                + "  \"p_type\"        VARCHAR(25), "
                + "  \"p_size\"        INT, "
                + "  \"p_container\"   CHAR(10), "
                + "  \"p_retailprice\" DECIMAL(15,2) , "
                + "  \"p_comment\"     VARCHAR(23) , "
                + "  \"p_dummy\"       VARCHAR(10), "
                + "  PRIMARY KEY (\"p_partkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"supplier\" ( "
                + "  \"s_suppkey\"     INT , "
                + "  \"s_name\"        CHAR(25) , "
                + "  \"s_address\"     VARCHAR(40) , "
                + "  \"s_nationkey\"   INT , "
                + "  \"s_phone\"       CHAR(15) , "
                + "  \"s_acctbal\"     DECIMAL(15,2) , "
                + "  \"s_comment\"     VARCHAR(101), "
                + "  \"s_dummy\" varchar(10), "
                + "  PRIMARY KEY (\"s_suppkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"partsupp\" ( "
                + "  \"ps_partkey\"     INT , "
                + "  \"ps_suppkey\"     INT , "
                + "  \"ps_availqty\"    INT , "
                + "  \"ps_supplycost\"  DECIMAL(15,2)  , "
                + "  \"ps_comment\"     VARCHAR(199), "
                + "  \"ps_dummy\"       VARCHAR(10), "
                + "  PRIMARY KEY (\"ps_partkey\", \"ps_suppkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE  IF NOT EXISTS \"%s\".\"customer\" ("
                + "  \"c_custkey\"     INT , "
                + "  \"c_name\"        VARCHAR(25) , "
                + "  \"c_address\"     VARCHAR(40) , "
                + "  \"c_nationkey\"   INT , "
                + "  \"c_phone\"       CHAR(15) , "
                + "  \"c_acctbal\"     DECIMAL(15,2)   , "
                + "  \"c_mktsegment\"  CHAR(10) , "
                + "  \"c_comment\"     VARCHAR(117), "
                + "  \"c_dummy\"       VARCHAR(10), "
                + "  PRIMARY KEY (\"c_custkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS  \"%s\".\"orders\"  ( "
                + "  \"o_orderkey\"       INT , "
                + "  \"o_custkey\"        INT , "
                + "  \"o_orderstatus\"    CHAR(1) , "
                + "  \"o_totalprice\"     DECIMAL(15,2) , "
                + "  \"o_orderdate\"      DATE , "
                + "  \"o_orderpriority\"  CHAR(15) , "
                + "  \"o_clerk\"          CHAR(15) , "
                + "  \"o_shippriority\"   INT , "
                + "  \"o_comment\"        VARCHAR(79), "
                + "  \"o_dummy\" varchar(10), "
                + "  PRIMARY KEY (\"o_orderkey\"))",
            schema));
    dbmsConn.execute(
        String.format(
            "CREATE TABLE IF NOT EXISTS \"%s\".\"lineitem\" ("
                + "  \"l_orderkey\"    INT , "
                + "  \"l_partkey\"     INT , "
                + "  \"l_suppkey\"     INT , "
                + "  \"l_linenumber\"  INT , "
                + "  \"l_quantity\"    DECIMAL(15,2) , "
                + "  \"l_extendedprice\"  DECIMAL(15,2) , "
                + "  \"l_discount\"    DECIMAL(15,2) , "
                + "  \"l_tax\"         DECIMAL(15,2) , "
                + "  \"l_returnflag\"  CHAR(1) , "
                + "  \"l_linestatus\"  CHAR(1) , "
                + "  \"l_shipdate\"    DATE , "
                + "  \"l_commitdate\"  DATE , "
                + "  \"l_receiptdate\" DATE , "
                + "  \"l_shipinstruct\" CHAR(25) , "
                + "  \"l_shipmode\"     CHAR(10) , "
                + "  \"l_comment\"      VARCHAR(44), "
                + "  \"l_dummy\" varchar(10))",
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
    dbmsConn.execute(
        String.format(
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
                + "xmlCol         xml,"
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
                + "serial8Col     serial8)",
            schema, table));

    dbmsConn.execute(
        String.format(
            "INSERT INTO \"%s\".\"%s\" VALUES ( "
                + "1, 1, '1', '1011', true, '((1,1), (2,2))', '1', '1234', '1234', "
                + "'10', '((1,1),2)', '2018-12-31', 1.0, '88.99.0.0/16', 1, "
                + "'{\"2\":1}', '{1,2,3}', '((1,1),(2,2))', "
                + "'08002b:010203', '08002b:0102030405', '12.34', 1.0, '((1,1))', '(1,1)', "
                + "'((1,1))', 1.0, 1, 1, 1, '1110', '2018-12-31 00:00:01', '2018-12-31 00:00:01', "
                + "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11',"
                + "'<foo>bar</foo>',"
                + " '1', 1, true, '1234', '1234', 1, 1, 1.0, 1.0, 1.0"
                + ", 1, 1, 1, '2018-12-31 00:00:01', '2018-12-31 00:00:01', 1)",
            schema, table));
    dbmsConn.execute(
        String.format(
            "INSERT INTO \"%s\".\"%s\" VALUES ( "
                + "NULL, 1, NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, NULL, NULL, "
                + "NULL, NULL, NULL, NULL, 1, 1, NULL, NULL, NULL, NULL,"
                + "NULL,"
                + "NULL, NULL, NULL, NULL, NULL, NULL, "
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
    dbmsConn.execute(
        String.format(
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
                + "timestampwtzCol timestamp without time zone, "
                + "timestamptzCol    timestamptz, "
                + "timestamptzCol2 timestamp with time zone)",
            schema, table));

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
    insertDataList.add("'2018-12-31 11:22:33'"); // timestamptz
    insertDataList.add("'2018-12-31 11:22:33'"); // timestamp with time zone

    dbmsConn.execute(
        String.format(
            "INSERT INTO \"%s\".\"%s\" VALUES (%s)",
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
    insertNullDataList.add("NULL"); // timestamptz
    insertNullDataList.add("NULL"); // timestamp with time zone

    dbmsConn.execute(
        String.format(
            "INSERT INTO \"%s\".\"%s\" VALUES (%s)",
            schema, table, Joiner.on(",").join(insertNullDataList)));

    return conn;
  }

  public static Connection setupPrestoForDataTypeTest(
      String connectionString, String user, String password, String schema, String table)
      throws SQLException, VerdictDBDbmsException {
    Connection conn = DriverManager.getConnection(connectionString, user, password);
    DbmsConnection dbmsConn = new JdbcConnection(conn, new PrestoHiveSyntax());

    dbmsConn.execute(String.format("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema));
    dbmsConn.execute(String.format("DROP TABLE IF EXISTS \"%s\".\"%s\"", schema, table));

    dbmsConn.execute(
        String.format(
            "CREATE TABLE \"%s\".\"%s\" ("
                + "tinyintCol          TINYINT,"
                + "boolCol             BOOLEAN,"
                + "smallintCol         SMALLINT,"
                + "integerCol          INTEGER,"
                + "bigintCol           BIGINT,"
                + "decimalCol          DECIMAL(4,2),"
                + "realCol             REAL,"
                + "doubleCol           DOUBLE,"
                + "dateCol             DATE,"
                + "timestampCol        TIMESTAMP,"
                + "charCol             CHAR(4),"
                + "varcharCol          VARCHAR(4))",
            schema, table));

    List<String> insertDataList = new ArrayList<>();
    insertDataList.add("cast(1 as tinyint)"); // tinyint
    insertDataList.add("true"); // boolean
    insertDataList.add("cast(2 as smallint)"); // smallint
    insertDataList.add("cast(3 as integer)"); // integer
    insertDataList.add("cast(4 as bigint)"); // bigint
    insertDataList.add("cast(5.0 as decimal(4,2))"); // decimal
    insertDataList.add("cast(1.0 as real)"); // real
    insertDataList.add("cast(1.0 as double)"); // double
    insertDataList.add("cast('2018-12-31' as date)"); // date
    insertDataList.add("cast('2018-12-31 00:00:01' as timestamp)"); // timestamp
    insertDataList.add("cast('ab' as char(4))"); // char
    insertDataList.add("cast('abcd' as varchar(4))"); // varchar

    dbmsConn.execute(
        String.format(
            "INSERT INTO \"%s\".\"%s\" VALUES (%s)",
            schema, table, Joiner.on(",").join(insertDataList)));

    List<String> insertNullDataList = new ArrayList<>();
    insertNullDataList.add("NULL"); // tinyint
    insertNullDataList.add("NULL"); // boolean
    insertNullDataList.add("NULL"); // smallint
    insertNullDataList.add("NULL"); // integer
    insertNullDataList.add("NULL"); // bigint
    insertNullDataList.add("NULL"); // decimal
    insertNullDataList.add("NULL"); // real
    insertNullDataList.add("NULL"); // double
    insertNullDataList.add("NULL"); // date
    insertNullDataList.add("NULL"); // timestamp
    insertNullDataList.add("NULL"); // char
    insertNullDataList.add("NULL"); // varchar

    dbmsConn.execute(
        String.format(
            "INSERT INTO \"%s\".\"%s\" VALUES (%s)",
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
    dbmsConn.execute(
        String.format(
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
                + "tinyintCol      tinyint, "
                + "varcharCol        varchar(10))",
            schema, table));

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
    insertDataList.add("cast('jackson' as varchar(10))"); // varchar

    dbmsConn.execute(
        String.format(
            "INSERT INTO `%s`.`%s` VALUES (%s)",
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
    insertNullDataList.add("NULL"); // varchar

    dbmsConn.execute(
        String.format(
            "INSERT INTO `%s`.`%s` VALUES (%s)",
            schema, table, Joiner.on(",").join(insertNullDataList)));

    return conn;
  }
}
