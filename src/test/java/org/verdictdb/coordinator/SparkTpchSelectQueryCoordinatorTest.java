package org.verdictdb.coordinator;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.commons.VerdictOption;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.SparkConnection;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

import static org.junit.Assert.assertEquals;

/**
 * Test cases are from
 * https://github.com/umich-dbgroup/verdictdb-core/wiki/TPCH-Query-Reference--(Experiment-Version)
 *
 * <p>Some test cases are slightly changed because size of test data are small.
 */
public class SparkTpchSelectQueryCoordinatorTest {

  static final int blockSize = 100;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static final String TEST_SCHEMA =
      "scrambling_coordinator_test_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();

  static DbmsConnection conn;

  static SparkSession spark;

  @BeforeClass
  public static void setupSpark() throws VerdictDBException {
    String appname = "scramblingCoordinatorTest";
    spark = DatabaseConnectionHelpers.setupSpark(appname, TEST_SCHEMA);
    conn = new SparkConnection(spark);

    // Create Scramble table
    conn.execute(String.format("DROP TABLE IF EXISTS `%s`.`lineitem_scrambled`", TEST_SCHEMA));
    conn.execute(String.format("DROP TABLE IF EXISTS `%s`.`orders_scrambled`", TEST_SCHEMA));

    ScramblingCoordinator scrambler =
        new ScramblingCoordinator(conn, TEST_SCHEMA, TEST_SCHEMA, (long) 200);
    ScrambleMeta meta1 =
        scrambler.scramble(TEST_SCHEMA, "lineitem", TEST_SCHEMA, "lineitem_scrambled", "uniform");
    ScrambleMeta meta2 =
        scrambler.scramble(TEST_SCHEMA, "orders", TEST_SCHEMA, "orders_scrambled", "uniform");
    meta.addScrambleMeta(meta1);
    meta.addScrambleMeta(meta2);
  }

  @Before
  public void setupSchema() {
    spark.sql(
        String.format(
            "drop schema if exists `%s` cascade", VerdictOption.getDefaultTempSchemaName()));
    spark.sql(
        String.format(
            "create schema if not exists `%s`", VerdictOption.getDefaultTempSchemaName()));
  }

  @Test
  public void testTpch1() throws VerdictDBException {
    String sql =
        "select "
            + " l_returnflag, "
            + " l_linestatus, "
            + " sum(l_quantity) as sum_qty, "
            + " sum(l_extendedprice) as sum_base_price, "
            + " sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, "
            + " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, "
            + " avg(l_quantity) as avg_qty, "
            + " avg(l_extendedprice) as avg_price, "
            + " avg(l_discount) as avg_disc, "
            + " count(*) as count_order "
            + "from "
            + " lineitem "
            + "where "
            + " l_shipdate <= date '1998-12-01'"
            + "group by "
            + " l_returnflag, "
            + " l_linestatus "
            + "order by "
            + " l_returnflag, "
            + " l_linestatus ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getString(1), dbmsQueryResult.getString(1));
          assertEquals(row.getDecimal(2).longValue(), dbmsQueryResult.getBigDecimal(2).longValue());
          assertEquals(
              row.getDecimal(3).doubleValue(),
              dbmsQueryResult.getBigDecimal(3).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(4).doubleValue(),
              dbmsQueryResult.getBigDecimal(4).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(5).doubleValue(),
              dbmsQueryResult.getBigDecimal(5).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(6).doubleValue(),
              dbmsQueryResult.getBigDecimal(6).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(7).doubleValue(),
              dbmsQueryResult.getBigDecimal(7).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(8).doubleValue(),
              dbmsQueryResult.getBigDecimal(8).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 1 finished");
  }

  @Test
  public void testTpch3() throws VerdictDBException {
    String sql =
        "select "
            + "l_orderkey, "
            + "sum(l_extendedprice * (1 - l_discount)) as revenue, "
            + "o_orderdate, "
            + "o_shippriority "
            + "from "
            + "customer, "
            + "orders, "
            + "lineitem "
            + "where "
            + "c_custkey = o_custkey "
            + "and l_orderkey = o_orderkey "
            + "and o_orderdate < date '1998-12-01' "
            + "and l_shipdate > date '1996-12-01' "
            + "group by "
            + "l_orderkey, "
            + "o_orderdate, "
            + "o_shippriority "
            + "order by "
            + "revenue desc, "
            + "o_orderdate "
            + "limit 10";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getInt(0), dbmsQueryResult.getInt(0));
          assertEquals(
              row.getDecimal(1).doubleValue(),
              dbmsQueryResult.getBigDecimal(1).doubleValue(),
              1e-5);
          assertEquals(row.getDate(2), dbmsQueryResult.getDate(2));
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 3 finished");
  }

  @Test
  public void testTpch4() throws VerdictDBException {
    String sql =
        "select "
            + "o_orderpriority, "
            + "count(*) as order_count "
            + "from "
            + "orders join lineitem on l_orderkey = o_orderkey "
            + "where "
            + "o_orderdate >= date '1992-12-01' "
            + "and o_orderdate < date '1998-12-01'"
            + "and l_commitdate < l_receiptdate "
            + "group by "
            + "o_orderpriority "
            + "order by "
            + "o_orderpriority ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getLong(1), dbmsQueryResult.getLong(1));
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 4 finished");
  }

  @Test
  public void testTpch5() throws VerdictDBException {
    String sql =
        "select "
            + "n_name, "
            + "sum(l_extendedprice * (1 - l_discount)) as revenue "
            + "from "
            + "customer, "
            + "orders, "
            + "lineitem, "
            + "supplier, "
            + "nation, "
            + "region "
            + "where "
            + "c_custkey = o_custkey "
            + "and l_orderkey = o_orderkey "
            + "and l_suppkey = s_suppkey "
            + "and c_nationkey = s_nationkey "
            + "and s_nationkey = n_nationkey "
            + "and n_regionkey = r_regionkey "
            + "and o_orderdate >= date '1992-12-01' "
            + "and o_orderdate < date '1998-12-01' "
            + "group by "
            + "n_name "
            + "order by "
            + "revenue desc ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(
              row.getDecimal(1).doubleValue(),
              dbmsQueryResult.getBigDecimal(1).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 5 finished");
  }

  @Test
  public void testTpch6() throws VerdictDBException {
    String sql =
        "select "
            + "sum(l_extendedprice * l_discount) as revenue "
            + "from "
            + "lineitem "
            + "where "
            + "l_shipdate >= date '1992-12-01' "
            + "and l_shipdate < date '1998-12-01' "
            + "and l_discount between 0.04 - 0.02 and 0.04 + 0.02 "
            + "and l_quantity < 15 ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(
              row.getDecimal(0).doubleValue(),
              dbmsQueryResult.getBigDecimal(0).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 6 finished");
  }

  @Test
  public void testTpch7() throws VerdictDBException {
    String sql =
        "select "
            + "supp_nation, "
            + "cust_nation, "
            + "l_year, "
            + "sum(volume) as revenue "
            + "from "
            + "( "
            + "select "
            + "n1.n_name as supp_nation, "
            + "n2.n_name as cust_nation, "
            + "substr(l_shipdate,0,4) as l_year, "
            + "l_extendedprice * (1 - l_discount) as volume "
            + "from "
            + "supplier, "
            + "lineitem, "
            + "orders, "
            + "customer, "
            + "nation n1, "
            + "nation n2 "
            + "where "
            + "s_suppkey = l_suppkey "
            + "and o_orderkey = l_orderkey "
            + "and c_custkey = o_custkey "
            + "and s_nationkey = n1.n_nationkey "
            + "and c_nationkey = n2.n_nationkey "
            + "and ( "
            + "(n1.n_name = 'CHINA' and n2.n_name = 'RUSSIA') "
            + "or (n1.n_name = 'RUSSIA' and n2.n_name = 'CHINA') "
            + ") "
            + "and l_shipdate between date '1992-01-01' and date '1996-12-31' "
            + ") as shipping "
            + "group by "
            + "supp_nation, "
            + "cust_nation, "
            + "l_year "
            + "order by "
            + "supp_nation, "
            + "cust_nation, "
            + "l_year ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getString(1), dbmsQueryResult.getString(1));
          assertEquals(row.getString(2), dbmsQueryResult.getString(2));
          assertEquals(
              row.getDecimal(3).doubleValue(),
              dbmsQueryResult.getBigDecimal(3).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 7 finished");
  }

  @Test
  public void testTpch8() throws VerdictDBException {
    String sql =
        "select\n"
            + "  o_year,\n"
            + "  sum(case\n"
            + "    when nation = 'PERU' then volume\n"
            + "    else 0\n"
            + "  end) as numerator, sum(volume) as demoninator\n"
            + "from\n"
            + "  (\n"
            + "    select\n"
            + "      year(o_orderdate) as o_year,\n"
            + "      l_extendedprice * (1 - l_discount) as volume,\n"
            + "      n2.n_name as nation\n"
            + "    from\n"
            + "      lineitem join orders on l_orderkey = o_orderkey\n"
            + "      join supplier on s_suppkey = l_suppkey\n"
            + "      join part on p_partkey = l_partkey\n"
            + "      join customer on o_custkey = c_custkey\n"
            + "      join nation n1 on c_nationkey = n1.n_nationkey\n"
            + "      join region on n1.n_regionkey = r_regionkey\n"
            + "      join nation n2 on s_nationkey = n2.n_nationkey\n"
            + "    where\n"
            + "      r_name = 'AMERICA'\n"
            + "      and o_orderdate between '1995-01-01' and '1996-12-31'\n"
            + "      and p_type = 'ECONOMY ANODIZED STEEL'"
            + "  ) as all_nations\n"
            + "group by\n"
            + "  o_year\n"
            + "order by\n"
            + "  o_year";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getInt(0), dbmsQueryResult.getInt(0));
          assertEquals(
              row.getDecimal(1).doubleValue(),
              dbmsQueryResult.getBigDecimal(1).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(2).doubleValue(),
              dbmsQueryResult.getBigDecimal(2).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 8 finished");
  }

  @Test
  public void testTpch9() throws VerdictDBException {
    String sql =
        "select\n"
            + "  nation,\n"
            + "  o_year,\n"
            + "  sum(amount) as sum_profit\n"
            + "from\n"
            + "  (\n"
            + "    select\n"
            + "      n_name as nation,\n"
            + "      year(o_orderdate) as o_year,\n"
            + "      l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\n"
            + "    from\n"
            + "      lineitem join orders on o_orderkey = l_orderkey\n"
            + "      join partsupp on ps_suppkey = l_suppkey and ps_partkey = l_partkey\n"
            + "      join supplier on s_suppkey = l_suppkey\n"
            + "      join part on p_partkey = l_partkey\n"
            + "      join nation on s_nationkey = n_nationkey\n"
            + "    where\n"
            + "      p_name like '%green%'\n"
            + "  ) as profit\n"
            + "group by\n"
            + "  nation,\n"
            + "  o_year\n"
            + "order by\n"
            + "  nation,\n"
            + "  o_year desc";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getInt(1), dbmsQueryResult.getInt(1));
          assertEquals(
              row.getDecimal(2).doubleValue(),
              dbmsQueryResult.getBigDecimal(2).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 9 finished");
  }

  @Test
  public void testTpch10() throws VerdictDBException {
    String sql =
        "select "
            + "c_custkey, "
            + "c_name, "
            + "sum(l_extendedprice * (1 - l_discount)) as revenue, "
            + "c_acctbal, "
            + "n_name, "
            + "c_address, "
            + "c_phone, "
            + "c_comment "
            + "from "
            + "customer, "
            + "orders, "
            + "lineitem, "
            + "nation "
            + "where "
            + "c_custkey = o_custkey "
            + "and l_orderkey = o_orderkey "
            + "and o_orderdate >= date '1992-01-01' "
            + "and o_orderdate < date '1998-01-01' "
            + "and l_returnflag = 'R' "
            + "and c_nationkey = n_nationkey "
            + "group by "
            + "c_custkey, "
            + "c_name, "
            + "c_acctbal, "
            + "c_phone, "
            + "n_name, "
            + "c_address, "
            + "c_comment "
            + "order by "
            + "revenue desc ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(1), dbmsQueryResult.getString(1));
          assertEquals(
              row.getDecimal(2).doubleValue(),
              dbmsQueryResult.getBigDecimal(2).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 10 finished");
  }

  @Test
  public void testTpch12() throws VerdictDBException {
    String sql =
        "select "
            + "l_shipmode, "
            + "sum(case "
            + "when o_orderpriority = '1-URGENT' "
            + "or o_orderpriority = '2-HIGH' "
            + "then 1 "
            + "else 0 "
            + "end) as high_line_count, "
            + "sum(case "
            + "when o_orderpriority <> '1-URGENT' "
            + "and o_orderpriority <> '2-HIGH' "
            + "then 1 "
            + "else 0 "
            + "end) as low_line_count "
            + "from "
            + "orders, "
            + "lineitem "
            + "where "
            + "o_orderkey = l_orderkey "
            + "and l_commitdate < l_receiptdate "
            + "and l_shipdate < l_commitdate "
            + "and l_receiptdate >= date '1992-01-01' "
            + "and l_receiptdate < date '1998-01-01' "
            + "group by "
            + "l_shipmode "
            + "order by "
            + "l_shipmode ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getLong(1), dbmsQueryResult.getLong(1));
          assertEquals(row.getLong(2), dbmsQueryResult.getLong(2));
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 12 finished");
  }

  @Test
  public void testTpch13() throws VerdictDBException {
    String sql =
        "select "
            + "c_custkey, "
            + "count(o_orderkey) as c_count "
            + "from "
            + "customer inner join orders on "
            + "c_custkey = o_custkey "
            + "and o_comment not like '%unusual%' "
            + "group by "
            + "c_custkey "
            + "order by c_custkey";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 2) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getLong(1), dbmsQueryResult.getLong(1));
        }
      }
    }
    assertEquals(2, cnt);
    System.out.println("test case 13 finished");
  }

  @Test
  public void testTpch14() throws VerdictDBException {
    String sql =
        "select "
            + "100.00 * sum(case "
            + "when p_type like 'PROMO%' "
            + "then l_extendedprice * (1 - l_discount) "
            + "else 0 "
            + "end) as numerator, sum(l_extendedprice * (1 - l_discount)) as denominator "
            + "from "
            + "lineitem, "
            + "part "
            + "where "
            + "l_partkey = p_partkey "
            + "and l_shipdate >= date '1992-01-01' "
            + "and l_shipdate < date '1998-01-01' ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(
              row.getDecimal(1).doubleValue(),
              dbmsQueryResult.getBigDecimal(1).doubleValue(),
              1e-5);
          assertEquals(
              row.getDecimal(0).doubleValue(),
              dbmsQueryResult.getBigDecimal(0).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 14 finished");
  }

  @Test
  public void testTpch15() throws VerdictDBException {
    String sql =
        "select "
            + "l_suppkey, "
            + "sum(l_extendedprice * (1 - l_discount)) "
            + "from "
            + "lineitem "
            + "where "
            + "l_shipdate >= date '1992-01-01' "
            + "and l_shipdate < date '1999-01-01'"
            + "group by "
            + "l_suppkey "
            + "order by "
            + "l_suppkey";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(
              row.getDecimal(1).doubleValue(),
              dbmsQueryResult.getBigDecimal(1).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 15 finished");
  }

  @Test
  public void testTpch17() throws VerdictDBException {
    String sql =
        "select\n"
            + "  sum(extendedprice) / 7.0 as avg_yearly\n"
            + "from (\n"
            + "  select\n"
            + "    l_quantity as quantity,\n"
            + "    l_extendedprice as extendedprice,\n"
            + "    t_avg_quantity\n"
            + "  from\n"
            + "    (select\n"
            + "  l_partkey,\n"
            + "  0.2 * avg(l_quantity) as t_avg_quantity\n"
            + "from\n"
            + "  lineitem\n"
            + "group by l_partkey) as q17_lineitem_tmp_cached Inner Join\n"
            + "    (select\n"
            + "      l_quantity,\n"
            + "      l_partkey,\n"
            + "      l_extendedprice\n"
            + "    from\n"
            + "      part,\n"
            + "      lineitem\n"
            + "    where\n"
            + "      p_partkey = l_partkey\n"
            + "    ) as l1 on l1.l_partkey = q17_lineitem_tmp_cached.l_partkey\n"
            + ") a \n"
            + "where quantity > t_avg_quantity";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(
              row.getDecimal(0).doubleValue(),
              dbmsQueryResult.getBigDecimal(0).doubleValue(),
              1e-1);
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 17 finished");
  }

  @Test
  public void testTpch18() throws VerdictDBException {
    String sql =
        "select\n"
            + "  c_name,\n"
            + "  c_custkey,\n"
            + "  o_orderkey,\n"
            + "  o_orderdate,\n"
            + "  o_totalprice,\n"
            + "  sum(l_quantity)\n"
            + "from\n"
            + "  customer,\n"
            + "  orders,\n"
            + "  (select\n"
            + "  l_orderkey,\n"
            + "  sum(l_quantity) as t_sum_quantity\n"
            + "  from\n"
            + "    lineitem\n"
            + "  where\n"
            + "    l_orderkey is not null\n"
            + "  group by\n"
            + "    l_orderkey) as t,\n"
            + "  lineitem l\n"
            + "where\n"
            + "  c_custkey = o_custkey\n"
            + "  and o_orderkey = t.l_orderkey\n"
            + "  and o_orderkey is not null\n"
            + "  and t.t_sum_quantity > 150\n"
            + "group by\n"
            + "  c_name,\n"
            + "  c_custkey,\n"
            + "  o_orderkey,\n"
            + "  o_orderdate,\n"
            + "  o_totalprice\n"
            + "order by\n"
            + "  o_totalprice desc,\n"
            + "  o_orderdate \n";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(
              row.getDecimal(5).doubleValue(),
              dbmsQueryResult.getBigDecimal(5).doubleValue(),
              1e-5);
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 18 finished");
  }

  @Test
  public void testTpch19() throws VerdictDBException {
    String sql =
        "select "
            + "sum(l_extendedprice* (1 - l_discount)) as revenue "
            + "from "
            + "lineitem, "
            + "part "
            + "where "
            + "( "
            + "p_partkey = l_partkey "
            + "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
            + "and l_quantity >= 4 and l_quantity <= 4 + 10 "
            + "and p_size between 1 and 5 "
            + "and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON' "
            + ") "
            + "or "
            + "( "
            + "p_partkey = l_partkey "
            + "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
            + "and l_quantity >= 5 and l_quantity <= 5 + 10 "
            + "and p_size between 1 and 10 "
            + "and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON' "
            + ") "
            + "or "
            + "( "
            + "p_partkey = l_partkey "
            + "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
            + "and l_quantity >= 6 and l_quantity <= 6 + 10 "
            + "and p_size between 1 and 15 "
            + "and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON' "
            + ") ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getDecimal(0), dbmsQueryResult.getValue(0));
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 19 finished");
  }

  @Test
  public void testTpch20() throws VerdictDBException {
    String sql =
        "select\n"
            + "  s_name,\n"
            + "  count(s_address)\n"
            + "from\n"
            + "  supplier,\n"
            + "  nation,\n"
            + "  partsupp,\n"
            + "  (select\n"
            + "    l_partkey,\n"
            + "    l_suppkey,\n"
            + "    0.5 * sum(l_quantity) as sum_quantity\n"
            + "  from\n"
            + "    lineitem\n"
            + "where\n"
            + "  l_shipdate >= '1994-01-01'\n"
            + "  and l_shipdate < '1998-01-01'\n"
            + "group by l_partkey, l_suppkey) as q20_tmp2_cached\n"
            + "where\n"
            + "  s_nationkey = n_nationkey\n"
            + "  and n_name = 'CANADA'\n"
            + "  and s_suppkey = ps_suppkey\n"
            + "  group by s_name\n"
            + "order by s_name";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 5) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getLong(1), dbmsQueryResult.getLong(1));
        }
      }
    }
    assertEquals(5, cnt);
    System.out.println("test case 20 finished");
  }

  @Test
  public void testTpch21() throws VerdictDBException {
    String sql =
        "select s_name, count(1) as numwait\n"
            + "from ("
            + "  select s_name "
            + "  from ("
            + "    select s_name, t2.l_orderkey, l_suppkey, count_suppkey, max_suppkey\n"
            + "    from ("
            + "      select l_orderkey, count(l_suppkey) count_suppkey, max(l_suppkey) as max_suppkey\n"
            + "      from lineitem\n"
            + "      where l_receiptdate > l_commitdate and l_orderkey is not null\n"
            + "      group by l_orderkey) as t2"
            + "    right outer join ("
            + "      select s_name as s_name, l_orderkey, l_suppkey "
            + "      from ("
            + "        select s_name as s_name, t1.l_orderkey, l_suppkey, count_suppkey, max_suppkey\n"
            + "        from ("
            + "          select l_orderkey, count(l_suppkey) as count_suppkey, max(l_suppkey) as max_suppkey\n"
            + "          from lineitem\n"
            + "          where l_orderkey is not null\n"
            + "          group by l_orderkey) as t1 "
            + "          join ("
            + "          select s_name, l_orderkey, l_suppkey\n"
            + "          from orders o join ("
            + "            select s_name, l_orderkey, l_suppkey\n"
            + "            from nation n join supplier s\n"
            + "              on s.s_nationkey = n.n_nationkey\n"
            + "            join lineitem l on s.s_suppkey = l.l_suppkey\n"
            + "          where l.l_receiptdate > l.l_commitdate\n"
            + "            and l.l_orderkey is not null) l1 "
            + "        on o.o_orderkey = l1.l_orderkey\n"
            + "          ) l2 on l2.l_orderkey = t1.l_orderkey\n"
            + "        ) a\n"
            + "      where (count_suppkey > 1) or ((count_suppkey=1) and (l_suppkey <> max_suppkey))\n"
            + "    ) l3 on l3.l_orderkey = t2.l_orderkey\n"
            + "  ) b\n"
            + "  where (count_suppkey is null) or ((count_suppkey=1) and (l_suppkey = max_suppkey))\n"
            + ") c "
            + "group by s_name "
            + "order by numwait desc, s_name ";

    String newSql = sql.replaceAll("lineitem", "lineitem_scrambled");
    newSql = newSql.replaceAll("orders", "orders_scrambled");

    DbmsConnection dbmsconn = new SparkConnection(spark);
    dbmsconn.setDefaultSchema(TEST_SCHEMA);
    SelectQueryCoordinator coordinator = new SelectQueryCoordinator(dbmsconn);

    coordinator.setScrambleMetaSet(meta);
    //    coordinator.setDefaultSchema("test");
    ExecutionResultReader reader = coordinator.process(newSql);

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(dbmsconn);
    relation = gen.standardize((SelectQuery) relation);

    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(new MysqlSyntax());
    String stdQuery = selectQueryToSql.toSql(relation);
    int cnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      cnt++;
      if (cnt == 6) {
        Dataset<Row> rs = spark.sql(stdQuery);
        for (Row row : rs.collectAsList()) {
          dbmsQueryResult.next();
          assertEquals(row.getString(0), dbmsQueryResult.getString(0));
          assertEquals(row.getLong(1), dbmsQueryResult.getLong(1));
        }
      }
    }
    assertEquals(6, cnt);
    System.out.println("test case 21 finished");
  }

  @AfterClass
  public static void tearDown() {
    spark.sql(String.format("DROP SCHEMA IF EXISTS %s CASCADE", TEST_SCHEMA));
  }
}
