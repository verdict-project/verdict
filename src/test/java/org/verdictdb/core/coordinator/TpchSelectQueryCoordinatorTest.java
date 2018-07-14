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
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *  Test cases are from
 *  https://github.com/umich-dbgroup/verdictdb-core/wiki/TPCH-Query-Reference--(Experiment-Version)
 *
 *  Some test cases are slightly changed because size of test data are small.
 */
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
        " l_linestatus ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getLong(3), dbmsQueryResult.getLong(2));
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-5);
          assertEquals(rs.getDouble(5), dbmsQueryResult.getDouble(4), 1e-5);
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-5);
          assertEquals(rs.getDouble(7), dbmsQueryResult.getDouble(6), 1e-5);
          assertEquals(rs.getDouble(8), dbmsQueryResult.getDouble(7), 1e-5);
          assertEquals(rs.getDouble(9), dbmsQueryResult.getDouble(8), 1e-5);
          assertEquals(rs.getDouble(10), dbmsQueryResult.getDouble(9), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 1 finished");
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
        "o_orderdate " +
        "limit 10";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 3 finished");
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

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 4 finished");
  }

  @Test
  public void test5Tpch() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "n_name, " +
        "sum(l_extendedprice * (1 - l_discount)) as revenue " +
        "from " +
        "customer, " +
        "orders_scrambled, " +
        "lineitem_scrambled, " +
        "supplier, " +
        "nation, " +
        "region " +
        "where " +
        "c_custkey = o_custkey " +
        "and l_orderkey = o_orderkey " +
        "and l_suppkey = s_suppkey " +
        "and c_nationkey = s_nationkey " +
        "and s_nationkey = n_nationkey " +
        "and n_regionkey = r_regionkey " +
        "and o_orderdate >= date '1992-12-01' " +
        "and o_orderdate < date '1998-12-01' " +
        "group by " +
        "n_name " +
        "order by " +
        "revenue desc ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 5 finished");
  }

  @Test
  public void test6Tpch() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "sum(l_extendedprice * l_discount) as revenue " +
        "from " +
        "lineitem_scrambled " +
        "where " +
        "l_shipdate >= date '1992-12-01' " +
        "and l_shipdate < date '1998-12-01' " +
        "and l_discount between 0.04 - 0.02 and 0.04 + 0.02 " +
        "and l_quantity < 15 ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 6 finished");
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

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getDouble(4), dbmsQueryResult.getDouble(3), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 7 finished");
  }

  @Test
  public void test8Tpch() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "o_year, " +
        "sum(case " +
        "when nation = 'PERU' then volume " +
        "else 0 " +
        "end) as numerator, sum(volume) as denominator " +
        "from " +
        "( " +
        "select " +
        "year(o_orderdate) as o_year, " +
        "l_extendedprice * (1 - l_discount) as volume, " +
        "n2.n_name as nation " +
        "from " +
        "part, " +
        "supplier, " +
        "lineitem_scrambled, " +
        "orders_scrambled, " +
        "customer, " +
        "nation n1, " +
        "nation n2, " +
        "region " +
        "where " +
        "p_partkey = l_partkey " +
        "and s_suppkey = l_suppkey " +
        "and l_orderkey = o_orderkey " +
        "and o_custkey = c_custkey " +
        "and c_nationkey = n1.n_nationkey " +
        "and n1.n_regionkey = r_regionkey " +
        "and r_name = 'AMERICA' " +
        "and s_nationkey = n2.n_nationkey " +
        "and o_orderdate between '1991-01-01' and '1996-12-31' " +
        ") as all_nations " +
        "group by " +
        "o_year " +
        "order by " +
        "o_year ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 8 finished");
  }

  @Test
  public void test9Tpch() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select " +
        "nation, " +
        "o_year, " +
        "sum(amount) as sum_profit " +
        "from " +
        "( " +
        "select " +
        "n_name as nation, " +
        "substr(o_orderdate,0,4) as o_year, " +
        "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount " +
        "from " +
        "part, " +
        "supplier, " +
        "lineitem_scrambled, " +
        "partsupp, " +
        "orders_scrambled, " +
        "nation " +
        "where " +
        "s_suppkey = l_suppkey " +
        "and ps_suppkey = l_suppkey " +
        "and ps_partkey = l_partkey " +
        "and p_partkey = l_partkey " +
        "and o_orderkey = l_orderkey " +
        "and s_nationkey = n_nationkey " +
        ") as profit " +
        "group by " +
        "nation, " +
        "o_year " +
        "order by " +
        "nation, " +
        "o_year desc ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 9 finished");
  }

  @Test
  public void test10Tpch() throws VerdictDBException, SQLException {
    String sql = "select " +
        "c_custkey, " +
        "c_name, " +
        "sum(l_extendedprice * (1 - l_discount)) as revenue, " +
        "c_acctbal, " +
        "n_name, " +
        "c_address, " +
        "c_phone, " +
        "c_comment " +
        "from " +
        "customer, " +
        "orders_scrambled, " +
        "lineitem_scrambled, " +
        "nation " +
        "where " +
        "c_custkey = o_custkey " +
        "and l_orderkey = o_orderkey " +
        "and o_orderdate >= date '1992-01-01' " +
        "and o_orderdate < date '1998-01-01' " +
        "and l_returnflag = 'R' " +
        "and c_nationkey = n_nationkey " +
        "group by " +
        "c_custkey, " +
        "c_name, " +
        "c_acctbal, " +
        "c_phone, " +
        "n_name, " +
        "c_address, " +
        "c_comment " +
        "order by " +
        "revenue desc ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 10 finished");
  }

  @Test
  public void test12Tpch() throws VerdictDBException, SQLException {
    String sql = "select " +
        "l_shipmode, " +
        "sum(case " +
        "when o_orderpriority = '1-URGENT' " +
        "or o_orderpriority = '2-HIGH' " +
        "then 1 " +
        "else 0 " +
        "end) as high_line_count, " +
        "sum(case " +
        "when o_orderpriority <> '1-URGENT' " +
        "and o_orderpriority <> '2-HIGH' " +
        "then 1 " +
        "else 0 " +
        "end) as low_line_count " +
        "from " +
        "orders_scrambled, " +
        "lineitem_scrambled " +
        "where " +
        "o_orderkey = l_orderkey " +
        "and l_commitdate < l_receiptdate " +
        "and l_shipdate < l_commitdate " +
        "and l_receiptdate >= date '1992-01-01' " +
        "and l_receiptdate < date '1998-01-01' " +
        "group by " +
        "l_shipmode " +
        "order by " +
        "l_shipmode ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(3), dbmsQueryResult.getDouble(2), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 12 finished");
  }

  @Test
  public void test13Tpch() throws VerdictDBException, SQLException {
    String sql = "select " +
        "c_custkey, " +
        "count(o_orderkey) as c_count " +
        "from " +
        "customer inner join orders_scrambled on " +
        "c_custkey = o_custkey " +
        "and o_comment not like '%unusual%' " +
        "group by " +
        "c_custkey " +
        "order by c_custkey";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 3) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(3, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 13 finished");
  }

  @Test
  public void test14Tpch() throws VerdictDBException, SQLException {
    String sql = "select " +
        "100.00 * sum(case " +
        "when p_type like 'PROMO%' " +
        "then l_extendedprice * (1 - l_discount) " +
        "else 0 " +
        "end) as numerator, sum(l_extendedprice * (1 - l_discount)) as denominator " +
        "from " +
        "lineitem_scrambled, " +
        "part " +
        "where " +
        "l_partkey = p_partkey " +
        "and l_shipdate >= date '1992-01-01' " +
        "and l_shipdate < date '1998-01-01' ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 14 finished");
  }

  @Test
  public void test15Tpch() throws VerdictDBException, SQLException {
    String sql = "select " +
        "l_suppkey, " +
        "sum(l_extendedprice * (1 - l_discount)) " +
        "from " +
        "lineitem_scrambled " +
        "where " +
        "l_shipdate >= date '1992-01-01' " +
        "and l_shipdate < date '1999-01-01'" +
        "group by " +
        "l_suppkey " +
        "order by " +
        "l_suppkey";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 15 finished");
  }

  @Test
  public void test17Tpch() throws VerdictDBException, SQLException {
    String sql = "select\n" +
        "  sum(extendedprice) / 7.0 as avg_yearly\n" +
        "from (\n" +
        "  select\n" +
        "    l_quantity as quantity,\n" +
        "    l_extendedprice as extendedprice,\n" +
        "    t_avg_quantity\n" +
        "  from\n" +
        "    (select\n" +
        "  l_partkey as t_partkey,\n" +
        "  0.2 * avg(l_quantity) as t_avg_quantity\n" +
        "from\n" +
        "  lineitem_scrambled\n" +
        "group by l_partkey) as q17_lineitem_tmp_cached Inner Join\n" +
        "    (select\n" +
        "      l_quantity,\n" +
        "      l_partkey,\n" +
        "      l_extendedprice\n" +
        "    from\n" +
        "      part,\n" +
        "      lineitem_scrambled\n" +
        "    where\n" +
        "      p_partkey = l_partkey\n" +
        "    ) as l1 on l1.l_partkey = t_partkey\n" +
        ") a \n" +
        "where quantity > t_avg_quantity";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 17 finished");
  }

  @Test
  public void test18Tpch() throws VerdictDBException, SQLException {
    String sql = "select\n" +
        "  c_name,\n" +
        "  c_custkey,\n" +
        "  o_orderkey,\n" +
        "  o_orderdate,\n" +
        "  o_totalprice,\n" +
        "  sum(l_quantity)\n" +
        "from\n" +
        "  customer,\n" +
        "  orders_scrambled,\n" +
        "  (select\n" +
        "  l_orderkey,\n" +
        "  sum(l_quantity) as t_sum_quantity\n" +
        "  from\n" +
        "    lineitem_scrambled\n" +
        "  where\n" +
        "    l_orderkey is not null\n" +
        "  group by\n" +
        "    l_orderkey) as t,\n" +
        "  lineitem_scrambled l\n" +
        "where\n" +
        "  c_custkey = o_custkey\n" +
        "  and o_orderkey = t.l_orderkey\n" +
        "  and o_orderkey is not null\n" +
        "  and t.t_sum_quantity > 150\n" +
        "group by\n" +
        "  c_name,\n" +
        "  c_custkey,\n" +
        "  o_orderkey,\n" +
        "  o_orderdate,\n" +
        "  o_totalprice\n" +
        "order by\n" +
        "  o_totalprice desc,\n" +
        "  o_orderdate \n";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getString(2), dbmsQueryResult.getString(1));
          assertEquals(rs.getString(3), dbmsQueryResult.getString(2));
          assertEquals(rs.getString(4), dbmsQueryResult.getString(3));
          assertEquals(rs.getString(5), dbmsQueryResult.getString(4));
          assertEquals(rs.getDouble(6), dbmsQueryResult.getDouble(5), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 18 finished");
  }

  @Test
  public void test19Tpch() throws VerdictDBException, SQLException {
    String sql = "select " +
        "sum(l_extendedprice* (1 - l_discount)) as revenue " +
        "from " +
        "lineitem_scrambled, " +
        "part " +
        "where " +
        "( " +
        "p_partkey = l_partkey " +
        "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') " +
        "and l_quantity >= 4 and l_quantity <= 4 + 10 " +
        "and p_size between 1 and 5 " +
        "and l_shipmode in ('AIR', 'AIR REG') " +
        "and l_shipinstruct = 'DELIVER IN PERSON' " +
        ") " +
        "or " +
        "( " +
        "p_partkey = l_partkey " +
        "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') " +
        "and l_quantity >= 5 and l_quantity <= 5 + 10 " +
        "and p_size between 1 and 10 " +
        "and l_shipmode in ('AIR', 'AIR REG') " +
        "and l_shipinstruct = 'DELIVER IN PERSON' " +
        ") " +
        "or " +
        "( " +
        "p_partkey = l_partkey " +
        "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') " +
        "and l_quantity >= 6 and l_quantity <= 6 + 10 " +
        "and p_size between 1 and 15 " +
        "and l_shipmode in ('AIR', 'AIR REG') " +
        "and l_shipinstruct = 'DELIVER IN PERSON' " +
        ") ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getDouble(1), dbmsQueryResult.getDouble(0), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 19 finished");
  }

  @Test
  public void test20Tpch() throws VerdictDBException, SQLException {
    String sql = "select\n" +
        "  s_name,\n" +
        "  count(s_address)\n" +
        "from\n" +
        "  supplier,\n" +
        "  nation,\n" +
        "  partsupp,\n" +
        "  (select\n" +
        "    l_partkey,\n" +
        "    l_suppkey,\n" +
        "    0.5 * sum(l_quantity) as sum_quantity\n" +
        "  from\n" +
        "    lineitem_scrambled\n" +
        "where\n" +
        "  l_shipdate >= '1994-01-01'\n" +
        "  and l_shipdate < '1998-01-01'\n" +
        "group by l_partkey, l_suppkey) as q20_tmp2_cached\n" +
        "where\n" +
        "  s_nationkey = n_nationkey\n" +
        "  and n_name = 'CANADA'\n" +
        "  and s_suppkey = ps_suppkey\n" +
        "  group by s_name\n" +
        "order by s_name";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 10) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(10, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 20 finished");
  }


  @Test
  public void test21Tpch() throws VerdictDBException, SQLException {
    String sql = "select s_name, count(1) as numwait\n" +
        "from (" +
        "  select s_name " +
        "  from (" +
        "    select s_name, t2.l_orderkey, l_suppkey, count_suppkey, max_suppkey\n" +
        "    from (" +
        "      select l_orderkey, count(l_suppkey) count_suppkey, max(l_suppkey) as max_suppkey\n" +
        "      from lineitem_scrambled\n" +
        "      where l_receiptdate > l_commitdate and l_orderkey is not null\n" +
        "      group by l_orderkey) as t2" +
        "    right outer join (" +
        "      select s_name as s_name, l_orderkey, l_suppkey " +
        "      from (" +
        "        select s_name as s_name, t1.l_orderkey, l_suppkey, count_suppkey, max_suppkey\n" +
        "        from (" +
        "          select l_orderkey, count(l_suppkey) as count_suppkey, max(l_suppkey) as max_suppkey\n" +
        "          from lineitem_scrambled\n" +
        "          where l_orderkey is not null\n" +
        "          group by l_orderkey) as t1 " +
        "          join (" +
        "          select s_name, l_orderkey, l_suppkey\n" +
        "          from orders_scrambled o join (" +
        "            select s_name, l_orderkey, l_suppkey\n" +
        "            from nation n join supplier s\n" +
        "              on s.s_nationkey = n.n_nationkey\n" +
        "            join lineitem_scrambled l on s.s_suppkey = l.l_suppkey\n" +
        "          where l.l_receiptdate > l.l_commitdate\n" +
        "            and l.l_orderkey is not null) l1 "
        + "        on o.o_orderkey = l1.l_orderkey\n" +
        "          ) l2 on l2.l_orderkey = t1.l_orderkey\n" +
        "        ) a\n" +
        "      where (count_suppkey > 1) or ((count_suppkey=1) and (l_suppkey <> max_suppkey))\n" +
        "    ) l3 on l3.l_orderkey = t2.l_orderkey\n" +
        "  ) b\n" +
        "  where (count_suppkey is null) or ((count_suppkey=1) and (l_suppkey = max_suppkey))\n" +
        ") c " +
        "group by s_name " +
        "order by numwait desc, s_name ";
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(new JdbcConnection(conn, new MysqlSyntax()));
    coordinator.setScrambleMetaSet(meta);
    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(sql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(coordinator.getStaticMetaData());
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 12) {
        ResultSet rs = stmt.executeQuery(stdQuery);
        while (rs.next()) {
          dbmsQueryResult.next();
          assertEquals(rs.getString(1), dbmsQueryResult.getString(0));
          assertEquals(rs.getDouble(2), dbmsQueryResult.getDouble(1), 1e-5);
        }
      }
    }
    assertEquals(12, cnt);
    stmt.execute("drop schema if exists `verdictdb_temp`;");
    System.out.println("test case 21 finished");
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
