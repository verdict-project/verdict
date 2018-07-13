package org.verdictdb.core.coordinator;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TpchSelectQueryCoordinatorTest {

  // lineitem has 10 blocks, orders has 3 blocks;
  // lineitem join orders has 12 blocks
  final static int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static Connection conn;

  private static Statement stmt;

  private static final String MYSQL_HOST;

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && env.equals("GitLab")) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s/%s?autoReconnect=true&useSSL=false", MYSQL_HOST, MYSQL_DATABASE);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`nation`  (`n_nationkey`  INT, " +
        "                            `n_name`       CHAR(25), " +
        "                            `n_regionkey`  INT, " +
        "                            `n_comment`    VARCHAR(152), " +
        "                            `n_dummy` varchar(10))");
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`region`  (`r_regionkey`  INT, " +
        "                            `r_name`       CHAR(25), " +
        "                            `r_comment`    VARCHAR(152), " +
        "                            `r_dummy` varchar(10))");
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`part`  ( `p_partkey`     INT, " +
        "                          `p_name`       VARCHAR(55), " +
        "                          `p_mfgr`        CHAR(25), " +
        "                          `p_brand`       CHAR(10), " +
        "                          `p_type`        VARCHAR(25), " +
        "                          `p_size`        INT, " +
        "                          `p_container`   CHAR(10), " +
        "                          `p_retailprice` DECIMAL(15,2) , " +
        "                          `p_comment`     VARCHAR(23) , " +
        "                          `p_dummy` varchar(10))");
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`supplier` ( `s_suppkey`     INT , " +
        "                             `s_name`        CHAR(25) , " +
        "                             `s_address`     VARCHAR(40) , " +
        "                             `s_nationkey`   INT , " +
        "                             `s_phone`       CHAR(15) , " +
        "                             `s_acctbal`     DECIMAL(15,2) , " +
        "                             `s_comment`     VARCHAR(101), " +
        "                             `s_dummy` varchar(10))");
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`partsupp` ( `ps_partkey`     INT , " +
        "                             `ps_suppkey`     INT , " +
        "                             `ps_availqty`    INT , " +
        "                             `ps_supplycost`  DECIMAL(15,2)  , " +
        "                             `ps_comment`     VARCHAR(199), " +
        "                             `ps_dummy` varchar(10))");
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`customer` ( `c_custkey`     INT , " +
        "                             `c_name`        VARCHAR(25) , " +
        "                             `c_address`     VARCHAR(40) , " +
        "                             `c_nationkey`   INT , " +
        "                             `c_phone`       CHAR(15) , " +
        "                             `c_acctbal`     DECIMAL(15,2)   , " +
        "                             `c_mktsegment`  CHAR(10) , " +
        "                             `c_comment`     VARCHAR(117), " +
        "                             `c_dummy` varchar(10))");
    stmt.execute("CREATE TABLE IF NOT EXISTS  `test`.`orders`  ( `o_orderkey`       INT , " +
        "                           `o_custkey`        INT , " +
        "                           `o_orderstatus`    CHAR(1) , " +
        "                           `o_totalprice`     DECIMAL(15,2) , " +
        "                           `o_orderdate`      DATE , " +
        "                           `o_orderpriority`  CHAR(15) , " +
        "                           `o_clerk`          CHAR(15) , " +
        "                           `o_shippriority`   INT , " +
        "                           `o_comment`        VARCHAR(79), " +
        "                           `o_dummy` varchar(10))");
    stmt.execute("CREATE TABLE  IF NOT EXISTS `test`.`lineitem` ( `l_orderkey`    INT , " +
        "                             `l_partkey`     INT , " +
        "                             `l_suppkey`     INT , " +
        "                             `l_linenumber`  INT , " +
        "                             `l_quantity`    DECIMAL(15,2) , " +
        "                             `l_extendedprice`  DECIMAL(15,2) , " +
        "                             `l_discount`    DECIMAL(15,2) , " +
        "                             `l_tax`         DECIMAL(15,2) , " +
        "                             `l_returnflag`  CHAR(1) , " +
        "                             `l_linestatus`  CHAR(1) , " +
        "                             `l_shipdate`    DATE , " +
        "                             `l_commitdate`  DATE , " +
        "                             `l_receiptdate` DATE , " +
        "                             `l_shipinstruct` CHAR(25) , " +
        "                             `l_shipmode`     CHAR(10) , " +
        "                             `l_comment`      VARCHAR(44), " +
        "                             `l_dummy` varchar(10))");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/region.tbl' " +
        "INTO TABLE `test`.`region` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/nation.tbl' " +
        "INTO TABLE `test`.`nation` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/supplier.tbl' " +
        "INTO TABLE `test`.`supplier` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/customer.tbl' " +
        "INTO TABLE `test`.`customer` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/part.tbl' " +
        "INTO TABLE `test`.`part` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/partsupp.tbl' " +
        "INTO TABLE `test`.`partsupp` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/lineitem.tbl' " +
        "INTO TABLE `test`.`lineitem` FIELDS TERMINATED BY '|'");
    stmt.execute("LOAD DATA LOCAL INFILE 'src/test/resources/tpch_test_data/orders.tbl' " +
        "INTO TABLE `test`.`orders` FIELDS TERMINATED BY '|'");


    // Create Scramble table
    ScramblingMethod method = new UniformScramblingMethod(blockSize);
    Map<String, String> options = new HashMap<>();
    options.put("tierColumnName", "verdictdbtier");
    options.put("blockColumnName", "verdictdbaggblock");
    ScramblingPlan plan = ScramblingPlan.create(
        "test", "lineitem_scrambled",
        "test", "lineitem",
        method, options);
    DbmsConnection mysqlConn = new JdbcConnection(conn, new MysqlSyntax());
    ExecutablePlanRunner.runTillEnd(mysqlConn, plan);
    ScramblingMethod method2 = new UniformScramblingMethod(blockSize);
    Map<String, String> options2 = new HashMap<>();
    options2.put("tierColumnName", "verdictdbtier");
    options2.put("blockColumnName", "verdictdbaggblock");
    ScramblingPlan plan2 = ScramblingPlan.create(
        "test", "orders_scrambled",
        "test", "orders",
        method2, options2);
    ExecutablePlanRunner.runTillEnd(mysqlConn, plan2);

    // Configure Sramble meta
    UniformScrambler scrambler =
        new UniformScrambler("test", "lineitem", "test", "lineitem_scrambled", 10);
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution1 = new HashMap<>();
    distribution1.put(0, Arrays.asList(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0));
    tablemeta.setCumulativeMassDistributionPerTier(distribution1);
    meta.insertScrambleMetaEntry(tablemeta);
    scrambler =
        new UniformScrambler("test", "orders", "test", "orders_scrambled", 3);
    tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(1);
    distribution1 = new HashMap<>();
    distribution1.put(0, Arrays.asList(0.33, 0.66, 1.0));
    tablemeta.setCumulativeMassDistributionPerTier(distribution1);
    meta.insertScrambleMetaEntry(tablemeta);
  }

  @Test
  public void testTpch1() throws VerdictDBException, SQLException {
    String sql = "select " +
        " l_returnflag, " +
        " l_linestatus, " +
        " sum(l_quantity) as sum_qty, " +
        " sum(l_extendedprice) as sum_base_price, " +
        " sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, " +
        " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, " +
        " avg(l_quantity) as avg_qty, " +
        " avg(l_extendedprice) as avg_price, " +
        " avg(l_discount) as avg_disc, " +
        " count(*) as count_order " +
        "from " +
        " lineitem_scrambled " +
        "where " +
        " l_shipdate <= date '1998-12-01'" +
        "group by " +
        " l_returnflag, " +
        " l_linestatus " +
        "order by " +
        " l_returnflag, " +
        " l_linestatus " +
        "LIMIT 1 ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      cnt++;
      if (cnt == 10) {
        assertEquals(true, dbmsQueryResult.getLong(2)==12400 || dbmsQueryResult.getLong(2)==5972
            || dbmsQueryResult.getLong(2)==6499 || dbmsQueryResult.getLong(2)==319);
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
  }

  @Test
  public void testTpch3() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "l_orderkey, " +
        "sum(l_extendedprice * (1 - l_discount)) as revenue, " +
        "o_orderdate, " +
        "o_shippriority " +
        "from " +
        "customer, " +
        "orders_scrambled, " +
        "lineitem_scrambled " +
        "where " +
        "c_custkey = o_custkey " +
        "and l_orderkey = o_orderkey " +
        "and o_orderdate < date '1998-12-01' " +
        "and l_shipdate > date '1996-12-01' " +
        "group by " +
        "l_orderkey, " +
        "o_orderdate, " +
        "o_shippriority " +
        "order by " +
        "revenue desc, " +
        "o_orderdate ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      cnt++;
      if (cnt == 12) {
        assertEquals(dbmsQueryResult.getRowCount(), 67);
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
  }

  @Test
  public void test4Tpch() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "o_orderpriority, " +
        "count(*) as order_count " +
        "from " +
        "orders_scrambled join lineitem_scrambled on l_orderkey = o_orderkey " +
        "where " +
        "o_orderdate >= date '1992-12-01' " +
        "and o_orderdate < date '1998-12-01'" +
        "and l_commitdate < l_receiptdate " +
        "group by " +
        "o_orderpriority " +
        "order by " +
        "o_orderpriority ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      cnt++;
      if (cnt == 12) {
        assertEquals(dbmsQueryResult.getRowCount(), 5);
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
  }

  @Test
  public void test7Tpch() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year, " +
        "sum(volume) as revenue " +
        "from " +
        "( " +
        "select " +
        "n1.n_name as supp_nation, " +
        "n2.n_name as cust_nation, " +
        "substr(l_shipdate,0,4) as l_year, " +
        "l_extendedprice * (1 - l_discount) as volume " +
        "from " +
        "supplier, " +
        "lineitem_scrambled, " +
        "orders_scrambled, " +
        "customer, " +
        "nation n1, " +
        "nation n2 " +
        "where " +
        "s_suppkey = l_suppkey " +
        "and o_orderkey = l_orderkey " +
        "and c_custkey = o_custkey " +
        "and s_nationkey = n1.n_nationkey " +
        "and c_nationkey = n2.n_nationkey " +
        "and ( " +
        "(n1.n_name = 'CHINA' and n2.n_name = 'RUSSIA') " +
        "or (n1.n_name = 'RUSSIA' and n2.n_name = 'CHINA') " +
        ") " +
        "and l_shipdate between date '1992-01-01' and date '1996-12-31' " +
        ") as shipping " +
        "group by " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year " +
        "order by " +
        "supp_nation, " +
        "cust_nation, " +
        "l_year ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      cnt++;
      if (cnt == 12) {
        assertEquals(dbmsQueryResult.getRowCount(), 2);
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("DROP TABLE IF EXISTS `test`.`region`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`nation`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`lineitem`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`customer`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`supplier`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`partsupp`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`part`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`orders`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`lineitem_scrambled`");
    stmt.execute("DROP TABLE IF EXISTS `test`.`orders_scrambled`");
  }
}
