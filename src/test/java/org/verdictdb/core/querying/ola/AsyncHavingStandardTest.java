package org.verdictdb.core.querying.ola;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.commons.DatabaseConnectionHelpers;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.coordinator.ScramblingCoordinator;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlwriter.QueryToSql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static java.sql.Types.BIGINT;

public class AsyncHavingStandardTest {


  final static int aggBlockCount = 2;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static Connection conn;

  static Statement stmt;


  private static StaticMetaData staticMetaData = new StaticMetaData();

  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:having;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    stmt = conn.createStatement();
    // create scrambled tables
//    int aggBlockCount = 2;
    stmt.execute("CREATE SCHEMA IF NOT EXISTS \"tpch\"");
    stmt.execute("CREATE TABLE  IF NOT EXISTS \"tpch\".\"lineitem\" ( \"l_orderkey\"    INT , " +
        "                             \"l_partkey\"     INT , " +
        "                             \"l_suppkey\"     INT , " +
        "                             \"l_linenumber\"  INT , " +
        "                             \"l_quantity\"    DECIMAL(15,2) , " +
        "                             \"l_extendedprice\"  DECIMAL(15,2) , " +
        "                             \"l_discount\"    DECIMAL(15,2) , " +
        "                             \"l_tax\"         DECIMAL(15,2) , " +
        "                             \"l_returnflag\"  CHAR(1) , " +
        "                             \"l_linestatus\"  CHAR(1) , " +
        "                             \"l_shipdate\"    DATE , " +
        "                             \"l_commitdate\"  DATE , " +
        "                             \"l_receiptdate\" DATE , " +
        "                             \"l_shipinstruct\" CHAR(25) , " +
        "                             \"l_shipmode\"     CHAR(10) , " +
        "                             \"l_comment\"      VARCHAR(44), " +
        "                             \"l_dummy\" varchar(10))");
    UniformScrambler scrambler =
        new UniformScrambler("tpch", "lineitem", "tpch", "lineitem_scrambled", aggBlockCount);
    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution1 = new HashMap<>();
    distribution1.put(0, Arrays.asList(0.2, 0.5, 1.0));
    tablemeta.setCumulativeDistributionForTier(distribution1);
    meta.addScrambleMeta(tablemeta);

    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("n_nationkey", BIGINT),
        new ImmutablePair<>("n_name", BIGINT),
        new ImmutablePair<>("n_regionkey", BIGINT),
        new ImmutablePair<>("n_comment", BIGINT)));
    staticMetaData.setDefaultSchema("tpch");
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "nation"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("r_regionkey", BIGINT),
        new ImmutablePair<>("r_name", BIGINT),
        new ImmutablePair<>("r_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "region"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("p_partkey", BIGINT),
        new ImmutablePair<>("p_name", BIGINT),
        new ImmutablePair<>("p_brand", BIGINT),
        new ImmutablePair<>("p_mfgr", BIGINT),
        new ImmutablePair<>("p_type", BIGINT),
        new ImmutablePair<>("p_size", BIGINT),
        new ImmutablePair<>("p_container", BIGINT),
        new ImmutablePair<>("p_retailprice", BIGINT),
        new ImmutablePair<>("p_comment", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "part"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_suppkey", BIGINT),
        new ImmutablePair<>("s_name", BIGINT),
        new ImmutablePair<>("s_address", BIGINT),
        new ImmutablePair<>("s_nationkey", BIGINT),
        new ImmutablePair<>("s_phone", BIGINT),
        new ImmutablePair<>("s_acctbal", BIGINT),
        new ImmutablePair<>("s_comment", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "supplier"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("ps_partkey", BIGINT),
        new ImmutablePair<>("ps_suppkey", BIGINT),
        new ImmutablePair<>("ps_availqty", BIGINT),
        new ImmutablePair<>("ps_supplycost", BIGINT),
        new ImmutablePair<>("ps_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "partsupp"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("c_custkey", BIGINT),
        new ImmutablePair<>("c_name", BIGINT),
        new ImmutablePair<>("c_address", BIGINT),
        new ImmutablePair<>("c_nationkey", BIGINT),
        new ImmutablePair<>("c_phone", BIGINT),
        new ImmutablePair<>("c_acctbal", BIGINT),
        new ImmutablePair<>("c_mktsegment", BIGINT),
        new ImmutablePair<>("c_comment", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "customer"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("o_orderkey", BIGINT),
        new ImmutablePair<>("o_custkey", BIGINT),
        new ImmutablePair<>("o_orderstatus", BIGINT),
        new ImmutablePair<>("o_totalprice", BIGINT),
        new ImmutablePair<>("o_orderdate", BIGINT),
        new ImmutablePair<>("o_orderpriority", BIGINT),
        new ImmutablePair<>("o_clerk", BIGINT),
        new ImmutablePair<>("o_shippriority", BIGINT),
        new ImmutablePair<>("o_comment", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "orders_scrambled"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("l_orderkey", BIGINT),
        new ImmutablePair<>("l_partkey", BIGINT),
        new ImmutablePair<>("l_suppkey", BIGINT),
        new ImmutablePair<>("l_linenumber", BIGINT),
        new ImmutablePair<>("l_quantity", BIGINT),
        new ImmutablePair<>("l_extendedprice", BIGINT),
        new ImmutablePair<>("l_discount", BIGINT),
        new ImmutablePair<>("l_tax", BIGINT),
        new ImmutablePair<>("l_returnflag", BIGINT),
        new ImmutablePair<>("l_linestatus", BIGINT),
        new ImmutablePair<>("l_shipdate", BIGINT),
        new ImmutablePair<>("l_commitdate", BIGINT),
        new ImmutablePair<>("l_receiptdate", BIGINT),
        new ImmutablePair<>("l_shipinstruct", BIGINT),
        new ImmutablePair<>("l_shipmode", BIGINT),
        new ImmutablePair<>("l_comment", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "lineitem_scrambled"), arr);
  }

  @Test
  public void test() throws VerdictDBException, SQLException {
    String sql = "select " +
        " l_returnflag, " +
        " l_linestatus, " +
        " l_quantity * 2 as qty, " +
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
        " l_quantity * 2, " +
        " l_returnflag, " +
        "l_linestatus " +
        "having " +
        "sum(l_extendedprice) > 5 " +
        "order by " +
        " l_quantity * 2, " +
        " sum(l_extendedprice * (1 - l_discount)) " +
        "LIMIT 1 ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

//    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan(new JdbcConnection(conn, new H2Syntax()),
//        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }
}
