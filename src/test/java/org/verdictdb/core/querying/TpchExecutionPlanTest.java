package org.verdictdb.core.querying;

import static java.sql.Types.BIGINT;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.AliasReference;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.GroupingAttribute;
import org.verdictdb.core.sqlobject.JoinTable;
import org.verdictdb.core.sqlobject.OrderbyAttribute;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.core.sqlobject.UnnamedColumn;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.H2Syntax;

/** Tests if a given query is properly converted into execution nodes. */
public class TpchExecutionPlanTest {

  static Connection conn;

  static Statement stmt;

  int aggblockCount = 2;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;

  String placeholderSchemaName = "placeholderSchemaName";

  String placeholderTableName = "placeholderTableName";

  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggexecnodetest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute("CREATE SCHEMA IF NOT EXISTS \"tpch\"");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"nation\"  (\"n_nationkey\"  INT, "
            + "                            \"n_name\"       CHAR(25), "
            + "                            \"n_regionkey\"  INT, "
            + "                            \"n_comment\"    VARCHAR(152), "
            + "                            \"n_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"region\"  (\"r_regionkey\"  INT, "
            + "                            \"r_name\"       CHAR(25), "
            + "                            \"r_comment\"    VARCHAR(152), "
            + "                            \"r_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"part\"  ( \"p_partkey\"     INT, "
            + "                          \"p_name\"       VARCHAR(55), "
            + "                          \"p_mfgr\"        CHAR(25), "
            + "                          \"p_brand\"       CHAR(10), "
            + "                          \"p_type\"        VARCHAR(25), "
            + "                          \"p_size\"        INT, "
            + "                          \"p_container\"   CHAR(10), "
            + "                          \"p_retailprice\" DECIMAL(15,2) , "
            + "                          \"p_comment\"     VARCHAR(23) , "
            + "                          \"p_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"supplier\" ( \"s_suppkey\"     INT , "
            + "                             \"s_name\"        CHAR(25) , "
            + "                             \"s_address\"     VARCHAR(40) , "
            + "                             \"s_nationkey\"   INT , "
            + "                             \"s_phone\"       CHAR(15) , "
            + "                             \"s_acctbal\"     DECIMAL(15,2) , "
            + "                             \"s_comment\"     VARCHAR(101), "
            + "                             \"s_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"partsupp\" ( \"ps_partkey\"     INT , "
            + "                             \"ps_suppkey\"     INT , "
            + "                             \"ps_availqty\"    INT , "
            + "                             \"ps_supplycost\"  DECIMAL(15,2)  , "
            + "                             \"ps_comment\"     VARCHAR(199), "
            + "                             \"ps_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"customer\" ( \"c_custkey\"     INT , "
            + "                             \"c_name\"        VARCHAR(25) , "
            + "                             \"c_address\"     VARCHAR(40) , "
            + "                             \"c_nationkey\"   INT , "
            + "                             \"c_phone\"       CHAR(15) , "
            + "                             \"c_acctbal\"     DECIMAL(15,2)   , "
            + "                             \"c_mktsegment\"  CHAR(10) , "
            + "                             \"c_comment\"     VARCHAR(117), "
            + "                             \"c_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE IF NOT EXISTS  \"tpch\".\"orders\"  ( \"o_orderkey\"       INT , "
            + "                           \"o_custkey\"        INT , "
            + "                           \"o_orderstatus\"    CHAR(1) , "
            + "                           \"o_totalprice\"     DECIMAL(15,2) , "
            + "                           \"o_orderdate\"      DATE , "
            + "                           \"o_orderpriority\"  CHAR(15) , "
            + "                           \"o_clerk\"          CHAR(15) , "
            + "                           \"o_shippriority\"   INT , "
            + "                           \"o_comment\"        VARCHAR(79), "
            + "                           \"o_dummy\" varchar(10))");
    stmt.execute(
        "CREATE TABLE  IF NOT EXISTS \"tpch\".\"lineitem\" ( \"l_orderkey\"    INT , "
            + "                             \"l_partkey\"     INT , "
            + "                             \"l_suppkey\"     INT , "
            + "                             \"l_linenumber\"  INT , "
            + "                             \"l_quantity\"    DECIMAL(15,2) , "
            + "                             \"l_extendedprice\"  DECIMAL(15,2) , "
            + "                             \"l_discount\"    DECIMAL(15,2) , "
            + "                             \"l_tax\"         DECIMAL(15,2) , "
            + "                             \"l_returnflag\"  CHAR(1) , "
            + "                             \"l_linestatus\"  CHAR(1) , "
            + "                             \"l_shipdate\"    DATE , "
            + "                             \"l_commitdate\"  DATE , "
            + "                             \"l_receiptdate\" DATE , "
            + "                             \"l_shipinstruct\" CHAR(25) , "
            + "                             \"l_shipmode\"     CHAR(10) , "
            + "                             \"l_comment\"      VARCHAR(44), "
            + "                             \"l_dummy\" varchar(10))");
    // create scrambled tables
    int aggBlockCount = 2;
    UniformScrambler scrambler =
        new UniformScrambler("tpch", "lineitem", "tpch", "lineitem", aggBlockCount);
    ScrambleMeta tablemeta = scrambler.generateMeta();
    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);
    scrambler = new UniformScrambler("tpch", "orders", "tpch", "orders", aggBlockCount);
    tablemeta = scrambler.generateMeta();
    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);

    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("n_nationkey", BIGINT),
            new ImmutablePair<>("n_name", BIGINT),
            new ImmutablePair<>("n_regionkey", BIGINT),
            new ImmutablePair<>("n_comment", BIGINT)));
    staticMetaData.setDefaultSchema("tpch");
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "nation"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("r_regionkey", BIGINT),
            new ImmutablePair<>("r_name", BIGINT),
            new ImmutablePair<>("r_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "region"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("p_partkey", BIGINT),
            new ImmutablePair<>("p_name", BIGINT),
            new ImmutablePair<>("p_brand", BIGINT),
            new ImmutablePair<>("p_mfgr", BIGINT),
            new ImmutablePair<>("p_type", BIGINT),
            new ImmutablePair<>("p_size", BIGINT),
            new ImmutablePair<>("p_container", BIGINT),
            new ImmutablePair<>("p_retailprice", BIGINT),
            new ImmutablePair<>("p_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "part"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("s_suppkey", BIGINT),
            new ImmutablePair<>("s_name", BIGINT),
            new ImmutablePair<>("s_address", BIGINT),
            new ImmutablePair<>("s_nationkey", BIGINT),
            new ImmutablePair<>("s_phone", BIGINT),
            new ImmutablePair<>("s_acctbal", BIGINT),
            new ImmutablePair<>("s_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "supplier"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("ps_partkey", BIGINT),
            new ImmutablePair<>("ps_suppkey", BIGINT),
            new ImmutablePair<>("ps_availqty", BIGINT),
            new ImmutablePair<>("ps_supplycost", BIGINT),
            new ImmutablePair<>("ps_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "partsupp"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("c_custkey", BIGINT),
            new ImmutablePair<>("c_name", BIGINT),
            new ImmutablePair<>("c_address", BIGINT),
            new ImmutablePair<>("c_nationkey", BIGINT),
            new ImmutablePair<>("c_phone", BIGINT),
            new ImmutablePair<>("c_acctbal", BIGINT),
            new ImmutablePair<>("c_mktsegment", BIGINT),
            new ImmutablePair<>("c_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "customer"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("o_orderkey", BIGINT),
            new ImmutablePair<>("o_custkey", BIGINT),
            new ImmutablePair<>("o_orderstatus", BIGINT),
            new ImmutablePair<>("o_totalprice", BIGINT),
            new ImmutablePair<>("o_orderdate", BIGINT),
            new ImmutablePair<>("o_orderpriority", BIGINT),
            new ImmutablePair<>("o_clerk", BIGINT),
            new ImmutablePair<>("o_shippriority", BIGINT),
            new ImmutablePair<>("o_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "orders"), arr);
    arr = new ArrayList<>();
    arr.addAll(
        Arrays.asList(
            new ImmutablePair<>("l_orderkey", BIGINT),
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
            new ImmutablePair<>("l_comment", BIGINT)));
    staticMetaData.addTableData(new StaticMetaData.TableInfo("tpch", "lineitem"), arr);
  }

  //  @Test
  public void Query1Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + " l_linestatus "
            + "LIMIT 1 ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(1, queryExecutionPlan.root.getExecutableNodeBaseDependents().size());
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependent(0)
            .getExecutableNodeBaseDependents()
            .size());

    BaseTable base = new BaseTable("tpch", "lineitem", "vt1");
    List<UnnamedColumn> operand1 =
        Arrays.<UnnamedColumn>asList(
            ConstantColumn.valueOf(1), new BaseColumn("vt1", "l_discount"));
    List<UnnamedColumn> operand2 =
        Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "l_extendedprice"),
            new ColumnOp("subtract", operand1));
    List<UnnamedColumn> operand3 =
        Arrays.<UnnamedColumn>asList(
            ConstantColumn.valueOf(1), new BaseColumn("vt1", "l_tax"));
    List<UnnamedColumn> operand4 =
        Arrays.<UnnamedColumn>asList(
            new ColumnOp("multiply", operand2), new ColumnOp("add", operand3));
    List<UnnamedColumn> operand5 =
        Arrays.<UnnamedColumn>asList(
            new BaseColumn("vt1", "l_shipdate"),
            new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")));
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt1", "l_returnflag"), "l_returnflag"),
                new AliasedColumn(
                    new BaseColumn("vt1", "l_linestatus"), "l_linestatus"),
                new AliasedColumn(
                    new ColumnOp("sum", new BaseColumn("vt1", "l_quantity")),
                    "sum_qty"),
                new AliasedColumn(
                    new ColumnOp(
                        "sum", new BaseColumn("vt1", "l_extendedprice")),
                    "sum_base_price"),
                new AliasedColumn(
                    new ColumnOp("sum", new ColumnOp("multiply", operand2)), "sum_disc_price"),
                new AliasedColumn(
                    new ColumnOp("sum", new ColumnOp("multiply", operand4)), "sum_charge"),
                new AliasedColumn(
                    new ColumnOp("avg", new BaseColumn("vt1", "l_quantity")),
                    "avg_qty"),
                new AliasedColumn(
                    new ColumnOp(
                        "avg", new BaseColumn("vt1", "l_extendedprice")),
                    "avg_price"),
                new AliasedColumn(
                    new ColumnOp("avg", new BaseColumn("vt1", "l_discount")),
                    "avg_disc"),
                new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "count_order")),
            base,
            new ColumnOp("lessequal", operand5));
    expected.addGroupby(
        Arrays.<GroupingAttribute>asList(
            new AliasReference("l_returnflag"), new AliasReference("l_linestatus")));
    expected.addOrderby(
        Arrays.<OrderbyAttribute>asList(
            new OrderbyAttribute("l_returnflag"), new OrderbyAttribute("l_linestatus")));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query3Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + "c_mktsegment = '123' "
            + "and c_custkey = o_custkey "
            + "and l_orderkey = o_orderkey "
            + "and o_orderdate < date '1998-12-01' "
            + "and l_shipdate > date '1998-12-01' "
            + "group by "
            + "l_orderkey, "
            + "o_orderdate, "
            + "o_shippriority "
            + "order by "
            + "revenue desc, "
            + "o_orderdate "
            + "LIMIT 10";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    ColumnOp op1 =
        new ColumnOp(
            "multiply",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_extendedprice"),
                new ColumnOp(
                    "subtract",
                    Arrays.<UnnamedColumn>asList(
                        ConstantColumn.valueOf(1),
                        new BaseColumn("vt3", "l_discount")))));
    ColumnOp revenue = new ColumnOp("sum", op1);
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt3", "l_orderkey"), "l_orderkey"),
                new AliasedColumn(revenue, "revenue"),
                new AliasedColumn(
                    new BaseColumn("vt2", "o_orderdate"), "o_orderdate"),
                new AliasedColumn(
                    new BaseColumn("vt2", "o_shippriority"), "o_shippriority"),
                new AliasedColumn(
                    new BaseColumn("vt3", "l_orderkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt2", "o_orderdate"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "1"),
                new AliasedColumn(
                    new BaseColumn("vt2", "o_shippriority"),
                    AsyncAggExecutionNode.getGroupByAlias() + "2"),
                new AliasedColumn(revenue, AsyncAggExecutionNode.getOrderByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt2", "o_orderdate"),
                    AsyncAggExecutionNode.getOrderByAlias() + "1")),
            Arrays.asList(customer, orders, lineitem));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_mktsegment"),
                ConstantColumn.valueOf("'123'"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_custkey"),
                new BaseColumn("vt2", "o_custkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_orderkey"),
                new BaseColumn("vt2", "o_orderkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "o_orderdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "greater",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")))));
    expected.addGroupby(
        Arrays.<GroupingAttribute>asList(
            new BaseColumn("vt3", "l_orderkey"),
            new BaseColumn("vt2", "o_orderdate"),
            new BaseColumn("vt2", "o_shippriority")));
    expected.addOrderby(
        Arrays.<OrderbyAttribute>asList(
            new OrderbyAttribute("revenue", "desc"), new OrderbyAttribute("o_orderdate")));
    expected.addLimit(ConstantColumn.valueOf(10));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query4Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "o_orderpriority, "
            + "count(*) as order_count "
            + "from "
            + "orders join lineitem on l_orderkey = o_orderkey "
            + "where "
            + "o_orderdate >= date '1998-12-01' "
            + "and o_orderdate < date '1998-12-01'"
            + "and l_commitdate < l_receiptdate "
            + "group by "
            + "o_orderpriority "
            + "order by "
            + "o_orderpriority "
            + "LIMIT 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation orders = new BaseTable("tpch", "orders", "vt1");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt1", "o_orderpriority"), "o_orderpriority"),
                new AliasedColumn(new ColumnOp("count", new AsteriskColumn()), "order_count"),
                new AliasedColumn(
                    new BaseColumn("vt1", "o_orderpriority"),
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "o_orderpriority"),
                    AsyncAggExecutionNode.getOrderByAlias() + "0")),
            orders);

    assertEquals(
        expected.getSelectList(),
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery.getSelectList());

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query5Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + "and r_name = '123' "
            + "and o_orderdate >= date '1998-12-01' "
            + "and o_orderdate < date '1998-12-01' "
            + "group by "
            + "n_name "
            + "order by "
            + "revenue desc "
            + "LIMIT 1 ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt4");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt5");
    AbstractRelation region = new BaseTable("tpch", "region", "vt6");
    ColumnOp revenue =
        new ColumnOp(
            "sum",
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "multiply",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt3", "l_extendedprice"),
                        new ColumnOp(
                            "subtract",
                            Arrays.<UnnamedColumn>asList(
                                ConstantColumn.valueOf(1),
                                new BaseColumn("vt3", "l_discount")))))));
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(new BaseColumn("vt5", "n_name"), "n_name"),
                new AliasedColumn(revenue, "revenue"),
                new AliasedColumn(
                    new BaseColumn("vt5", "n_name"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(revenue, AsyncAggExecutionNode.getOrderByAlias() + "0")),
            Arrays.asList(customer, orders, lineitem, supplier, nation, region));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_custkey"),
                new BaseColumn("vt2", "o_custkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_orderkey"),
                new BaseColumn("vt2", "o_orderkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_suppkey"),
                new BaseColumn("vt4", "s_suppkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_nationkey"),
                new BaseColumn("vt4", "s_nationkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt4", "s_nationkey"),
                new BaseColumn("vt5", "n_nationkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "n_regionkey"),
                new BaseColumn("vt6", "r_regionkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt6", "r_name"),
                ConstantColumn.valueOf("'123'"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "o_orderdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "o_orderdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")))));
    expected.addGroupby(new BaseColumn("vt5", "n_name"));
    expected.addOrderby(new OrderbyAttribute("revenue", "desc"));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query6Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "sum(l_extendedprice * l_discount) as revenue "
            + "from "
            + "lineitem "
            + "where "
            + "l_shipdate >= date '1998-12-01' "
            + "and l_shipdate < date '1998-12-01' "
            + "and l_discount between 0.04 - 0.01 and 0.04 + 0.01 "
            + "and l_quantity < 15 "
            + "LIMIT 1; ";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "multiply",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "l_extendedprice"),
                                new BaseColumn("vt1", "l_discount")))),
                    "revenue")),
            lineitem);
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-12-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "between",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_discount"),
                new ColumnOp(
                    "subtract",
                    Arrays.<UnnamedColumn>asList(
                        ConstantColumn.valueOf("0.04"), ConstantColumn.valueOf("0.01"))),
                new ColumnOp(
                    "add",
                    Arrays.<UnnamedColumn>asList(
                        ConstantColumn.valueOf("0.04"), ConstantColumn.valueOf("0.01"))))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_quantity"),
                ConstantColumn.valueOf("15"))));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query7Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + "(n1.n_name = ':1' and n2.n_name = ':2') "
            + "or (n1.n_name = ':2' and n2.n_name = ':1') "
            + ") "
            + "and l_shipdate between date '1995-01-01' and date '1996-12-31' "
            + ") as shipping "
            + "group by "
            + "supp_nation, "
            + "cust_nation, "
            + "l_year "
            + "order by "
            + "supp_nation, "
            + "cust_nation, "
            + "l_year "
            + "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt4");
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt5");
    AbstractRelation nation1 = new BaseTable("tpch", "nation", "vt6");
    AbstractRelation nation2 = new BaseTable("tpch", "nation", "vt7");
    SelectQuery subquery =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(new BaseColumn("vt6", "n_name"), "supp_nation"),
                new AliasedColumn(new BaseColumn("vt7", "n_name"), "cust_nation"),
                new AliasedColumn(
                    new ColumnOp(
                        "substr",
                        Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt3", "l_shipdate"),
                            ConstantColumn.valueOf(0),
                            ConstantColumn.valueOf(4))),
                    "l_year"),
                new AliasedColumn(
                    new ColumnOp(
                        "multiply",
                        Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt3", "l_extendedprice"),
                            new ColumnOp(
                                "subtract",
                                Arrays.<UnnamedColumn>asList(
                                    ConstantColumn.valueOf(1),
                                    new BaseColumn("vt3", "l_discount"))))),
                    "volume")),
            Arrays.asList(supplier, lineitem, orders, customer, nation1, nation2));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "s_suppkey"),
                new BaseColumn("vt3", "l_suppkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt4", "o_orderkey"),
                new BaseColumn("vt3", "l_orderkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "c_custkey"),
                new BaseColumn("vt4", "o_custkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "s_nationkey"),
                new BaseColumn("vt6", "n_nationkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "c_nationkey"),
                new BaseColumn("vt7", "n_nationkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "or",
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "and",
                    Arrays.<UnnamedColumn>asList(
                        new ColumnOp(
                            "equal",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt6", "n_name"),
                                ConstantColumn.valueOf("':1'"))),
                        new ColumnOp(
                            "equal",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt7", "n_name"),
                                ConstantColumn.valueOf("':2'"))))),
                new ColumnOp(
                    "and",
                    Arrays.<UnnamedColumn>asList(
                        new ColumnOp(
                            "equal",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt6", "n_name"),
                                ConstantColumn.valueOf("':2'"))),
                        new ColumnOp(
                            "equal",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt7", "n_name"),
                                ConstantColumn.valueOf("':1'"))))))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "between",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1995-01-01'")),
                new ColumnOp("date", ConstantColumn.valueOf("'1996-12-31'")))));
    subquery.setAliasName("vt1");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(new BaseColumn("vt1", "supp_nation"), "supp_nation"),
                new AliasedColumn(new BaseColumn("vt1", "cust_nation"), "cust_nation"),
                new AliasedColumn(new BaseColumn("vt1", "l_year"), "l_year"),
                new AliasedColumn(
                    new ColumnOp("sum", new BaseColumn("vt1", "volume")), "revenue"),
                new AliasedColumn(
                    new BaseColumn("vt1", "supp_nation"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "cust_nation"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "1"),
                new AliasedColumn(
                    new BaseColumn("vt1", "l_year"), AsyncAggExecutionNode.getGroupByAlias() + "2"),
                new AliasedColumn(
                    new BaseColumn("vt1", "supp_nation"),
                    AsyncAggExecutionNode.getOrderByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "cust_nation"),
                    AsyncAggExecutionNode.getOrderByAlias() + "1"),
                new AliasedColumn(
                    new BaseColumn("vt1", "l_year"),
                    AsyncAggExecutionNode.getOrderByAlias() + "2")),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt1"));
    expected.addGroupby(
        Arrays.<GroupingAttribute>asList(
            new BaseColumn("vt1", "supp_nation"),
            new BaseColumn("vt1", "cust_nation"),
            new BaseColumn("vt1", "l_year")));
    expected.addOrderby(
        Arrays.<OrderbyAttribute>asList(
            new OrderbyAttribute("supp_nation"),
            new OrderbyAttribute("cust_nation"),
            new OrderbyAttribute("l_year")));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);
    assertEquals(
        subquery,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query8Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "o_year, "
            + "sum(case "
            + "when nation = 'PERU' then volume "
            + "else 0 "
            + "end) as numerator, sum(volume) as denominator "
            + "from "
            + "( "
            + "select "
            + "year(o_orderdate) as o_year, "
            + "l_extendedprice * (1 - l_discount) as volume, "
            + "n2.n_name as nation "
            + "from "
            + "part, "
            + "supplier, "
            + "lineitem, "
            + "orders, "
            + "customer, "
            + "nation n1, "
            + "nation n2, "
            + "region "
            + "where "
            + "p_partkey = l_partkey "
            + "and s_suppkey = l_suppkey "
            + "and l_orderkey = o_orderkey "
            + "and o_custkey = c_custkey "
            + "and c_nationkey = n1.n_nationkey "
            + "and n1.n_regionkey = r_regionkey "
            + "and r_name = 'AMERICA' "
            + "and s_nationkey = n2.n_nationkey "
            + "and o_orderdate between '1995-01-01' and '1996-12-31' "
            + "and p_type = 'ECONOMY BURNISHED NICKEL' "
            + ") as all_nations "
            + "group by "
            + "o_year "
            + "order by "
            + "o_year "
            + "Limit 1";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt3");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt4");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt5");
    AbstractRelation customer = new BaseTable("tpch", "customer", "vt6");
    AbstractRelation nation1 = new BaseTable("tpch", "nation", "vt7");
    AbstractRelation nation2 = new BaseTable("tpch", "nation", "vt8");
    AbstractRelation region = new BaseTable("tpch", "region", "vt9");
    SelectQuery subquery =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new ColumnOp(
                        "year", 
                        new BaseColumn("vt5", "o_orderdate")),
                    "o_year"),
                new AliasedColumn(
                    new ColumnOp(
                        "multiply",
                        Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt4", "l_extendedprice"),
                            new ColumnOp(
                                "subtract",
                                Arrays.<UnnamedColumn>asList(
                                    ConstantColumn.valueOf(1),
                                    new BaseColumn("vt4", "l_discount"))))),
                    "volume"),
                new AliasedColumn(new BaseColumn("vt8", "n_name"), "nation")),
            Arrays.asList(part, supplier, lineitem, orders, customer, nation1, nation2, region));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "p_partkey"),
                new BaseColumn("vt4", "l_partkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "s_suppkey"),
                new BaseColumn("vt4", "l_suppkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt4", "l_orderkey"),
                new BaseColumn("vt5", "o_orderkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "o_custkey"),
                new BaseColumn("vt6", "c_custkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt6", "c_nationkey"),
                new BaseColumn("vt7", "n_nationkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt7", "n_regionkey"),
                new BaseColumn("vt9", "r_regionkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt9", "r_name"),
                ConstantColumn.valueOf("'AMERICA'"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "s_nationkey"),
                new BaseColumn("vt8", "n_nationkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "between",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "o_orderdate"),
                ConstantColumn.valueOf("'1995-01-01'"),
                ConstantColumn.valueOf("'1996-12-31'"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "p_type"),
                ConstantColumn.valueOf("'ECONOMY BURNISHED NICKEL'"))));
    subquery.setAliasName("vt1");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(new BaseColumn("vt1", "o_year"), "o_year"),
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "casewhen",
                            Arrays.<UnnamedColumn>asList(
                                new ColumnOp(
                                    "equal",
                                    Arrays.<UnnamedColumn>asList(
                                        new BaseColumn("vt1", "nation"),
                                        ConstantColumn.valueOf("'PERU'"))),
                                new BaseColumn("vt1", "volume"),
                                ConstantColumn.valueOf(0)))),
                    "numerator"),
                new AliasedColumn(
                    new ColumnOp("sum", new BaseColumn("vt1", "volume")), "denominator"),
                new AliasedColumn(
                    new BaseColumn("vt1", "o_year"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "o_year"),
                    AsyncAggExecutionNode.getOrderByAlias() + "0")),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt1"));
    expected.addGroupby(new BaseColumn("vt1", "o_year"));
    expected.addOrderby(new OrderbyAttribute("o_year"));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);
    assertEquals(
        subquery,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query9Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "nation, "
            + "o_year, "
            + "sum(amount) as sum_profit "
            + "from "
            + "( "
            + "select "
            + "n_name as nation, "
            + "substr(o_orderdate,0,4) as o_year, "
            + "l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount "
            + "from "
            + "part, "
            + "supplier, "
            + "lineitem, "
            + "partsupp, "
            + "orders, "
            + "nation "
            + "where "
            + "s_suppkey = l_suppkey "
            + "and ps_suppkey = l_suppkey "
            + "and ps_partkey = l_partkey "
            + "and p_partkey = l_partkey "
            + "and o_orderkey = l_orderkey "
            + "and s_nationkey = n_nationkey "
            + "and p_name like '%:1%' "
            + ") as profit "
            + "group by "
            + "nation, "
            + "o_year "
            + "order by "
            + "nation, "
            + "o_year desc "
            + "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    AbstractRelation supplier = new BaseTable("tpch", "supplier", "vt3");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt4");
    AbstractRelation partsupp = new BaseTable("tpch", "partsupp", "vt5");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt6");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt7");
    BaseColumn nationColumn = new BaseColumn("vt7", "n_name");
    SelectQuery subquery =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(nationColumn, "nation"),
                new AliasedColumn(
                    new ColumnOp(
                        "substr",
                        Arrays.<UnnamedColumn>asList(
                            new BaseColumn("vt6", "o_orderdate"),
                            ConstantColumn.valueOf(0),
                            ConstantColumn.valueOf(4))),
                    "o_year"),
                new AliasedColumn(
                    new ColumnOp(
                        "subtract",
                        Arrays.<UnnamedColumn>asList(
                            new ColumnOp(
                                "multiply",
                                Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt4", "l_extendedprice"),
                                    new ColumnOp(
                                        "subtract",
                                        Arrays.<UnnamedColumn>asList(
                                            ConstantColumn.valueOf(1),
                                            new BaseColumn("vt4", "l_discount"))))),
                            new ColumnOp(
                                "multiply",
                                Arrays.<UnnamedColumn>asList(
                                    new BaseColumn("vt5", "ps_supplycost"),
                                    new BaseColumn("vt4", "l_quantity"))))),
                    "amount")),
            Arrays.asList(part, supplier, lineitem, partsupp, orders, nation));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "s_suppkey"),
                new BaseColumn("vt4", "l_suppkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "ps_suppkey"),
                new BaseColumn("vt4", "l_suppkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt5", "ps_partkey"),
                new BaseColumn("vt4", "l_partkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "p_partkey"),
                new BaseColumn("vt4", "l_partkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt6", "o_orderkey"),
                new BaseColumn("vt4", "l_orderkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "s_nationkey"),
                new BaseColumn("vt7", "n_nationkey"))));
    subquery.addFilterByAnd(
        new ColumnOp(
            "like",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "p_name"),
                ConstantColumn.valueOf("'%:1%'"))));
    subquery.setAliasName("vt1");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(new BaseColumn("vt1", "nation"), "nation"),
                new AliasedColumn(new BaseColumn("vt1", "o_year"), "o_year"),
                new AliasedColumn(
                    new ColumnOp("sum", new BaseColumn("vt1", "amount")), "sum_profit"),
                new AliasedColumn(
                    new BaseColumn("vt1", "nation"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "o_year"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "1"),
                new AliasedColumn(
                    new BaseColumn("vt1", "nation"),
                    AsyncAggExecutionNode.getOrderByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "o_year"),
                    AsyncAggExecutionNode.getOrderByAlias() + "1")),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt1"));
    expected.addGroupby(
        Arrays.<GroupingAttribute>asList(
            new BaseColumn("vt1", "nation"),
            new BaseColumn("vt1", "o_year")));
    expected.addOrderby(
        Arrays.<OrderbyAttribute>asList(
            new OrderbyAttribute("nation"), new OrderbyAttribute("o_year", "desc")));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);
    assertEquals(
        subquery,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query10Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + "and o_orderdate >= date '2018-01-01' "
            + "and o_orderdate < date '2018-01-01' "
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
            + "revenue desc "
            + "LIMIT 20;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation customer = new BaseTable("tpch", "customer", "vt1");
    AbstractRelation orders = new BaseTable("tpch", "orders", "vt2");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt3");
    AbstractRelation nation = new BaseTable("tpch", "nation", "vt4");
    AliasedColumn revenue =
        new AliasedColumn(
            new ColumnOp(
                "sum",
                new ColumnOp(
                    "multiply",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt3", "l_extendedprice"),
                        new ColumnOp(
                            "subtract",
                            Arrays.<UnnamedColumn>asList(
                                ConstantColumn.valueOf(1),
                                new BaseColumn("vt3", "l_discount")))))),
            "revenue");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt1", "c_custkey"), "c_custkey"),
                new AliasedColumn(new BaseColumn("vt1", "c_name"), "c_name"),
                revenue,
                new AliasedColumn(
                    new BaseColumn("vt1", "c_acctbal"), "c_acctbal"),
                new AliasedColumn(new BaseColumn("vt4", "n_name"), "n_name"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_address"), "c_address"),
                new AliasedColumn(new BaseColumn("vt1", "c_phone"), "c_phone"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_comment"), "c_comment"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_custkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_name"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "1"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_acctbal"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "2"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_phone"),
                    AsyncAggExecutionNode.getGroupByAlias() + "3"),
                new AliasedColumn(
                    new BaseColumn("vt4", "n_name"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "4"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_address"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "5"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_comment"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "6"),
                new AliasedColumn(
                    revenue.getColumn(), AsyncAggExecutionNode.getOrderByAlias() + "0")),
            Arrays.asList(customer, orders, lineitem, nation));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_custkey"),
                new BaseColumn("vt2", "o_custkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_orderkey"),
                new BaseColumn("vt2", "o_orderkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "o_orderdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'2018-01-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "o_orderdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'2018-01-01'")))));

    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt3", "l_returnflag"),
                ConstantColumn.valueOf("'R'"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "c_nationkey"),
                new BaseColumn("vt4", "n_nationkey"))));
    expected.addGroupby(
        Arrays.<GroupingAttribute>asList(
            new BaseColumn("vt1", "c_custkey"),
            new BaseColumn("vt1", "c_name"),
            new BaseColumn("vt1", "c_acctbal"),
            new BaseColumn("vt1", "c_phone"),
            new BaseColumn("vt4", "n_name"),
            new BaseColumn("vt1", "c_address"),
            new BaseColumn("vt1", "c_comment")));
    expected.addOrderby(new OrderbyAttribute("revenue", "desc"));
    expected.addLimit(ConstantColumn.valueOf(20));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query12Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + "and l_shipmode in (':1', ':2') "
            + "and l_commitdate < l_receiptdate "
            + "and l_shipdate < l_commitdate "
            + "and l_receiptdate >= date '2018-01-01' "
            + "and l_receiptdate < date '2018-01-01' "
            + "group by "
            + "l_shipmode "
            + "order by "
            + "l_shipmode "
            + "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation orders = new BaseTable("tpch", "orders", "vt1");
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt2");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(new BaseColumn("vt2", "l_shipmode"), "vc3"),
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "casewhen",
                            Arrays.<UnnamedColumn>asList(
                                new ColumnOp(
                                    "or",
                                    Arrays.<UnnamedColumn>asList(
                                        new ColumnOp(
                                            "equal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn(
                                                    "tpch", "orders", "vt1", "o_orderpriority"),
                                                ConstantColumn.valueOf("'1-URGENT'"))),
                                        new ColumnOp(
                                            "equal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn(
                                                    "tpch", "orders", "vt1", "o_orderpriority"),
                                                ConstantColumn.valueOf("'2-HIGH'"))))),
                                ConstantColumn.valueOf(1),
                                ConstantColumn.valueOf(0)))),
                    "high_line_count"),
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "casewhen",
                            Arrays.<UnnamedColumn>asList(
                                new ColumnOp(
                                    "and",
                                    Arrays.<UnnamedColumn>asList(
                                        new ColumnOp(
                                            "notequal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn(
                                                    "tpch", "orders", "vt1", "o_orderpriority"),
                                                ConstantColumn.valueOf("'1-URGENT'"))),
                                        new ColumnOp(
                                            "notequal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn(
                                                    "tpch", "orders", "vt1", "o_orderpriority"),
                                                ConstantColumn.valueOf("'2-HIGH'"))))),
                                ConstantColumn.valueOf(1),
                                ConstantColumn.valueOf(0)))),
                    "low_line_count")),
            Arrays.asList(orders, lineitem));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "o_orderkey"),
                new BaseColumn("vt2", "l_orderkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "in",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_shipmode"),
                ConstantColumn.valueOf("':1'"),
                ConstantColumn.valueOf("':2'"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_commitdate"),
                new BaseColumn("vt2", "l_receiptdate"))));

    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_shipdate"),
                new BaseColumn("vt2", "l_commitdate"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_receiptdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'2018-01-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt2", "l_receiptdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'2018-01-01'")))));
    expected.addGroupby(new AliasReference("vc3"));
    expected.addOrderby(new OrderbyAttribute("vc3"));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        relation,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void SimplifiedQuery13Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "c_custkey, "
            + "count(o_orderkey) as c_count "
            + "from "
            + "customer left outer join orders on "
            + "c_custkey = o_custkey "
            + "and o_comment not like '%unusual%accounts%' "
            + "group by "
            + "c_custkey";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    BaseTable customer = new BaseTable("tpch", "customer", "vt1");
    BaseTable orders = new BaseTable("tpch", "orders", "vt2");
    JoinTable join =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(customer, orders),
            Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.leftouter),
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "and",
                    Arrays.<UnnamedColumn>asList(
                        new ColumnOp(
                            "equal",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "c_custkey"),
                                new BaseColumn("vt2", "o_custkey"))),
                        new ColumnOp(
                            "notlike",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt2", "o_comment"),
                                ConstantColumn.valueOf("'%unusual%accounts%'")))))));
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt1", "c_custkey"), "c_custkey"),
                new AliasedColumn(
                    new ColumnOp("count", new BaseColumn("vt2", "o_orderkey")),
                    "c_count"),
                new AliasedColumn(
                    new BaseColumn("vt1", "c_custkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0")),
            join);
    expected.addGroupby(new BaseColumn("vt1", "c_custkey"));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query14Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
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
            + "and l_shipdate >= date '2018-01-01' "
            + "and l_shipdate < date '2018-01-01' "
            + "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new ColumnOp(
                        "multiply",
                        Arrays.<UnnamedColumn>asList(
                            ConstantColumn.valueOf("100.00"),
                            new ColumnOp(
                                "sum",
                                new ColumnOp(
                                    "casewhen",
                                    Arrays.<UnnamedColumn>asList(
                                        new ColumnOp(
                                            "like",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn("vt2", "p_type"),
                                                ConstantColumn.valueOf("'PROMO%'"))),
                                        new ColumnOp(
                                            "multiply",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn(
                                                    "tpch", "lineitem", "vt1", "l_extendedprice"),
                                                new ColumnOp(
                                                    "subtract",
                                                    Arrays.<UnnamedColumn>asList(
                                                        ConstantColumn.valueOf(1),
                                                        new BaseColumn(
                                                            "tpch",
                                                            "lineitem",
                                                            "vt1",
                                                            "l_discount"))))),
                                        ConstantColumn.valueOf(0)))))),
                    "numerator"),
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "multiply",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "l_extendedprice"),
                                new ColumnOp(
                                    "subtract",
                                    Arrays.<UnnamedColumn>asList(
                                        ConstantColumn.valueOf(1),
                                        new BaseColumn(
                                            "tpch", "lineitem", "vt1", "l_discount")))))),
                    "denominator")),
            Arrays.asList(lineitem, part));
    expected.addFilterByAnd(
        new ColumnOp(
            "equal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_partkey"),
                new BaseColumn("vt2", "p_partkey"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'2018-01-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.<UnnamedColumn>asList(
                new BaseColumn("vt1", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'2018-01-01'")))));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        relation,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  // Use the view
  @Test
  public void IncompleteQuery15Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "l_suppkey, "
            + "sum(l_extendedprice * (1 - l_discount)) "
            + "from "
            + "lineitem "
            + "where "
            + "l_shipdate >= date '1998-01-01' "
            + "and l_shipdate < date '1999-01-01'"
            + "group by "
            + "l_suppkey";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt1", "l_suppkey"), "l_suppkey"),
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "multiply",
                            Arrays.asList(
                                new BaseColumn("vt1", "l_extendedprice"),
                                new ColumnOp(
                                    "subtract",
                                    Arrays.asList(
                                        ConstantColumn.valueOf(1),
                                        new BaseColumn("vt1", "l_discount")))))),
                    "s2"),
                new AliasedColumn(
                    new BaseColumn("vt1", "l_suppkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0")),
            lineitem);
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.asList(
                new BaseColumn("vt1", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1998-01-01'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.asList(
                new BaseColumn("vt1", "l_shipdate"),
                new ColumnOp("date", ConstantColumn.valueOf("'1999-01-01'")))));
    expected.addGroupby(new BaseColumn("vt1", "l_suppkey"));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .getSelectQuery());

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  // TODO: check the converted structure.
  public void Query17Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();

    String sql =
        "select\n"
            + "\tsum(extendedprice) / 7.0 as avg_yearly\n"
            + "from (\n"
            + "\tselect\n"
            + "\t\tl_quantity as quantity,\n"
            + "\t\tl_extendedprice as extendedprice,\n"
            + "\t\tt_avg_quantity\n"
            + "\tfrom\n"
            + "\t\t(select\n"
            + "\tl_partkey as t_partkey,\n"
            + "\t0.2 * avg(l_quantity) as t_avg_quantity\n"
            + "from\n"
            + "\tlineitem\n"
            + "group by l_partkey) as q17_lineitem_tmp_cached Inner Join\n"
            + "\t\t(select\n"
            + "\t\t\tl_quantity,\n"
            + "\t\t\tl_partkey,\n"
            + "\t\t\tl_extendedprice\n"
            + "\t\tfrom\n"
            + "\t\t\tpart,\n"
            + "\t\t\tlineitem\n"
            + "\t\twhere\n"
            + "\t\t\tp_partkey = l_partkey\n"
            + "\t\t\tand p_brand = 'Brand#23'\n"
            + "\t\t\tand p_container = 'MED BOX'\n"
            + "\t\t) as l1 on l1.l_partkey = t_partkey\n"
            + ") a \n"
            + "where quantity < t_avg_quantity";

    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        2,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    assertEquals(
        new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt1"),
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    JoinTable join =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(
                new BaseTable("placeholderSchema_2_0", "placeholderTable_2_0", "vt2"),
                new BaseTable("placeholderSchema_2_1", "placeholderTable_2_1", "vt4")),
            Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.inner),
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "equal",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt4", "l_partkey"),
                        new BaseColumn("vt2", "t_partkey")))));
    assertEquals(
        join,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt3", "l_partkey"), "t_partkey"),
                new AliasedColumn(
                    new ColumnOp(
                        "multiply",
                        Arrays.asList(
                            ConstantColumn.valueOf(0.2),
                            new ColumnOp(
                                "avg", new BaseColumn("vt3", "l_quantity")))),
                    "t_avg_quantity"),
                new AliasedColumn(
                    new BaseColumn("l_partkey"), AsyncAggExecutionNode.getGroupByAlias() + "0")),
            new BaseTable("tpch", "lineitem", "vt3"));
    expected.addGroupby(new BaseColumn("l_partkey"));
    expected.setAliasName("vt2");
    assertEquals(
        expected,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query18Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select\n"
            + "\tc_name,\n"
            + "\tc_custkey,\n"
            + "\to_orderkey,\n"
            + "\to_orderdate,\n"
            + "\to_totalprice,\n"
            + "\tsum(l_quantity)\n"
            + "from\n"
            + "\tcustomer,\n"
            + "\torders,\n"
            + "\t(select\n"
            + "\tl_orderkey,\n"
            + "\tsum(l_quantity) as t_sum_quantity\n"
            + "from\n"
            + "\tlineitem\n"
            + "where\n"
            + "\tl_orderkey is not null\n"
            + "group by\n"
            + "\tl_orderkey) as t,\n"
            + "\tlineitem l\n"
            + "where\n"
            + "\tc_custkey = o_custkey\n"
            + "\tand o_orderkey = t.l_orderkey\n"
            + "\tand o_orderkey is not null\n"
            + "\tand t.t_sum_quantity > 300\n"
            + "group by\n"
            + "\tc_name,\n"
            + "\tc_custkey,\n"
            + "\to_orderkey,\n"
            + "\to_orderdate,\n"
            + "\to_totalprice\n"
            + "order by\n"
            + "\to_totalprice desc,\n"
            + "\to_orderdate \n"
            + "limit 100;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());

    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt4", "l_orderkey"), "l_orderkey"),
                new AliasedColumn(
                    new ColumnOp("sum", new BaseColumn("vt4", "l_quantity")),
                    "t_sum_quantity"),
                new AliasedColumn(
                    new BaseColumn("vt4", "l_orderkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0")),
            new BaseTable("tpch", "lineitem", "vt4"));
    expected.addGroupby(new BaseColumn("vt4", "l_orderkey"));
    expected.setAliasName("vt3");
    expected.addFilterByAnd(
        new ColumnOp("is_not_null", new BaseColumn("vt4", "l_orderkey")));

    assertEquals(
        expected,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);
    assertEquals(
        new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt3"),
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .getSelectQuery()
            .getFromList()
            .get(2));

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query19Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select "
            + "sum(l_extendedprice* (1 - l_discount)) as revenue "
            + "from "
            + "lineitem, "
            + "part "
            + "where "
            + "( "
            + "p_partkey = l_partkey "
            + "and p_brand = ':1' "
            + "and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') "
            + "and l_quantity >= 4 and l_quantity <= 4 + 10 "
            + "and p_size between 1 and 5 "
            + "and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON' "
            + ") "
            + "or "
            + "( "
            + "p_partkey = l_partkey "
            + "and p_brand = ':2' "
            + "and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') "
            + "and l_quantity >= 5 and l_quantity <= 5 + 10 "
            + "and p_size between 1 and 10 "
            + "and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON' "
            + ") "
            + "or "
            + "( "
            + "p_partkey = l_partkey "
            + "and p_brand = ':3' "
            + "and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') "
            + "and l_quantity >= 6 and l_quantity <= 6 + 10 "
            + "and p_size between 1 and 15 "
            + "and l_shipmode in ('AIR', 'AIR REG') "
            + "and l_shipinstruct = 'DELIVER IN PERSON' "
            + ") "
            + "LIMIT 1;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    AbstractRelation lineitem = new BaseTable("tpch", "lineitem", "vt1");
    AbstractRelation part = new BaseTable("tpch", "part", "vt2");
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new ColumnOp(
                        "sum",
                        new ColumnOp(
                            "multiply",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "l_extendedprice"),
                                new ColumnOp(
                                    "subtract",
                                    Arrays.<UnnamedColumn>asList(
                                        ConstantColumn.valueOf(1),
                                        new BaseColumn("vt1", "l_discount")))))),
                    "revenue")),
            Arrays.asList(lineitem, part));
    ColumnOp columnOp1 =
        new ColumnOp(
            "and",
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "and",
                    Arrays.<UnnamedColumn>asList(
                        new ColumnOp(
                            "and",
                            Arrays.<UnnamedColumn>asList(
                                new ColumnOp(
                                    "and",
                                    Arrays.<UnnamedColumn>asList(
                                        new ColumnOp(
                                            "and",
                                            Arrays.<UnnamedColumn>asList(
                                                new ColumnOp(
                                                    "and",
                                                    Arrays.<UnnamedColumn>asList(
                                                        new ColumnOp(
                                                            "and",
                                                            Arrays.<UnnamedColumn>asList(
                                                                new ColumnOp(
                                                                    "equal",
                                                                    Arrays.<UnnamedColumn>asList(
                                                                        new BaseColumn(
                                                                            "vt2",
                                                                            "p_partkey"),
                                                                        new BaseColumn(
                                                                            "vt1",
                                                                            "l_partkey"))),
                                                                new ColumnOp(
                                                                    "equal",
                                                                    Arrays.<UnnamedColumn>asList(
                                                                        new BaseColumn(
                                                                            "vt2",
                                                                            "p_brand"),
                                                                        ConstantColumn.valueOf(
                                                                            "':1'"))))),
                                                        new ColumnOp(
                                                            "in",
                                                            Arrays.<UnnamedColumn>asList(
                                                                new BaseColumn(
                                                                    "vt2",
                                                                    "p_container"),
                                                                ConstantColumn.valueOf("'SM CASE'"),
                                                                ConstantColumn.valueOf("'SM BOX'"),
                                                                ConstantColumn.valueOf("'SM PACK'"),
                                                                ConstantColumn.valueOf(
                                                                    "'SM PKG'"))))),
                                                new ColumnOp(
                                                    "greaterequal",
                                                    Arrays.<UnnamedColumn>asList(
                                                        new BaseColumn(
                                                            "vt1",
                                                            "l_quantity"),
                                                        ConstantColumn.valueOf(4))))),
                                        new ColumnOp(
                                            "lessequal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn("vt1", "l_quantity"),
                                                new ColumnOp(
                                                    "add",
                                                    Arrays.<UnnamedColumn>asList(
                                                        ConstantColumn.valueOf(4),
                                                        ConstantColumn.valueOf(10))))))),
                                new ColumnOp(
                                    "between",
                                    Arrays.<UnnamedColumn>asList(
                                        new BaseColumn("vt2", "p_size"),
                                        ConstantColumn.valueOf(1),
                                        ConstantColumn.valueOf(5))))),
                        new ColumnOp(
                            "in",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "l_shipmode"),
                                ConstantColumn.valueOf("'AIR'"),
                                ConstantColumn.valueOf("'AIR REG'"))))),
                new ColumnOp(
                    "equal",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "l_shipinstruct"),
                        ConstantColumn.valueOf("'DELIVER IN PERSON'")))));
    ColumnOp columnOp2 =
        new ColumnOp(
            "and",
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "and",
                    Arrays.<UnnamedColumn>asList(
                        new ColumnOp(
                            "and",
                            Arrays.<UnnamedColumn>asList(
                                new ColumnOp(
                                    "and",
                                    Arrays.<UnnamedColumn>asList(
                                        new ColumnOp(
                                            "and",
                                            Arrays.<UnnamedColumn>asList(
                                                new ColumnOp(
                                                    "and",
                                                    Arrays.<UnnamedColumn>asList(
                                                        new ColumnOp(
                                                            "and",
                                                            Arrays.<UnnamedColumn>asList(
                                                                new ColumnOp(
                                                                    "equal",
                                                                    Arrays.<UnnamedColumn>asList(
                                                                        new BaseColumn(
                                                                            "vt2",
                                                                            "p_partkey"),
                                                                        new BaseColumn(
                                                                            "vt1",
                                                                            "l_partkey"))),
                                                                new ColumnOp(
                                                                    "equal",
                                                                    Arrays.<UnnamedColumn>asList(
                                                                        new BaseColumn(
                                                                            "vt2",
                                                                            "p_brand"),
                                                                        ConstantColumn.valueOf(
                                                                            "':2'"))))),
                                                        new ColumnOp(
                                                            "in",
                                                            Arrays.<UnnamedColumn>asList(
                                                                new BaseColumn(
                                                                    "vt2",
                                                                    "p_container"),
                                                                ConstantColumn.valueOf("'MED BAG'"),
                                                                ConstantColumn.valueOf("'MED BOX'"),
                                                                ConstantColumn.valueOf("'MED PKG'"),
                                                                ConstantColumn.valueOf(
                                                                    "'MED PACK'"))))),
                                                new ColumnOp(
                                                    "greaterequal",
                                                    Arrays.<UnnamedColumn>asList(
                                                        new BaseColumn(
                                                            "vt1",
                                                            "l_quantity"),
                                                        ConstantColumn.valueOf(5))))),
                                        new ColumnOp(
                                            "lessequal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn("vt1", "l_quantity"),
                                                new ColumnOp(
                                                    "add",
                                                    Arrays.<UnnamedColumn>asList(
                                                        ConstantColumn.valueOf(5),
                                                        ConstantColumn.valueOf(10))))))),
                                new ColumnOp(
                                    "between",
                                    Arrays.<UnnamedColumn>asList(
                                        new BaseColumn("vt2", "p_size"),
                                        ConstantColumn.valueOf(1),
                                        ConstantColumn.valueOf(10))))),
                        new ColumnOp(
                            "in",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "l_shipmode"),
                                ConstantColumn.valueOf("'AIR'"),
                                ConstantColumn.valueOf("'AIR REG'"))))),
                new ColumnOp(
                    "equal",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "l_shipinstruct"),
                        ConstantColumn.valueOf("'DELIVER IN PERSON'")))));
    ColumnOp columnOp3 =
        new ColumnOp(
            "and",
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "and",
                    Arrays.<UnnamedColumn>asList(
                        new ColumnOp(
                            "and",
                            Arrays.<UnnamedColumn>asList(
                                new ColumnOp(
                                    "and",
                                    Arrays.<UnnamedColumn>asList(
                                        new ColumnOp(
                                            "and",
                                            Arrays.<UnnamedColumn>asList(
                                                new ColumnOp(
                                                    "and",
                                                    Arrays.<UnnamedColumn>asList(
                                                        new ColumnOp(
                                                            "and",
                                                            Arrays.<UnnamedColumn>asList(
                                                                new ColumnOp(
                                                                    "equal",
                                                                    Arrays.<UnnamedColumn>asList(
                                                                        new BaseColumn(
                                                                            "vt2",
                                                                            "p_partkey"),
                                                                        new BaseColumn(
                                                                            "vt1",
                                                                            "l_partkey"))),
                                                                new ColumnOp(
                                                                    "equal",
                                                                    Arrays.<UnnamedColumn>asList(
                                                                        new BaseColumn(
                                                                            "vt2",
                                                                            "p_brand"),
                                                                        ConstantColumn.valueOf(
                                                                            "':3'"))))),
                                                        new ColumnOp(
                                                            "in",
                                                            Arrays.<UnnamedColumn>asList(
                                                                new BaseColumn(
                                                                    "vt2",
                                                                    "p_container"),
                                                                ConstantColumn.valueOf("'LG CASE'"),
                                                                ConstantColumn.valueOf("'LG BOX'"),
                                                                ConstantColumn.valueOf("'LG PACK'"),
                                                                ConstantColumn.valueOf(
                                                                    "'LG PKG'"))))),
                                                new ColumnOp(
                                                    "greaterequal",
                                                    Arrays.<UnnamedColumn>asList(
                                                        new BaseColumn(
                                                            "vt1",
                                                            "l_quantity"),
                                                        ConstantColumn.valueOf(6))))),
                                        new ColumnOp(
                                            "lessequal",
                                            Arrays.<UnnamedColumn>asList(
                                                new BaseColumn("vt1", "l_quantity"),
                                                new ColumnOp(
                                                    "add",
                                                    Arrays.<UnnamedColumn>asList(
                                                        ConstantColumn.valueOf(6),
                                                        ConstantColumn.valueOf(10))))))),
                                new ColumnOp(
                                    "between",
                                    Arrays.<UnnamedColumn>asList(
                                        new BaseColumn("vt2", "p_size"),
                                        ConstantColumn.valueOf(1),
                                        ConstantColumn.valueOf(15))))),
                        new ColumnOp(
                            "in",
                            Arrays.<UnnamedColumn>asList(
                                new BaseColumn("vt1", "l_shipmode"),
                                ConstantColumn.valueOf("'AIR'"),
                                ConstantColumn.valueOf("'AIR REG'"))))),
                new ColumnOp(
                    "equal",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt1", "l_shipinstruct"),
                        ConstantColumn.valueOf("'DELIVER IN PERSON'")))));
    expected.addFilterByAnd(
        new ColumnOp(
            "or",
            Arrays.<UnnamedColumn>asList(
                new ColumnOp("or", Arrays.<UnnamedColumn>asList(columnOp1, columnOp2)),
                columnOp3)));
    expected.addLimit(ConstantColumn.valueOf(1));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .selectQuery);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  // Query 20 is not a aggregated function. Change to count(s_address)
  @Test
  public void Query20Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select\n"
            + "\ts_name,\n"
            + "\tcount(s_address)\n"
            + "from\n"
            + "\tsupplier,\n"
            + "\tnation,\n"
            + "\tpartsupp,\n"
            + "\t(select\n"
            + "\tl_partkey,\n"
            + "\tl_suppkey,\n"
            + "\t0.5 * sum(l_quantity) as sum_quantity\n"
            + "from\n"
            + "\tlineitem\n"
            + "where\n"
            + "\tl_shipdate >= '1994-01-01'\n"
            + "\tand l_shipdate < '1995-01-01'\n"
            + "group by l_partkey, l_suppkey) as q20_tmp2_cached\n"
            + "where\n"
            + "\ts_nationkey = n_nationkey\n"
            + "\tand n_name = 'CANADA'\n"
            + "\tand s_suppkey = ps_suppkey\n"
            + "\tgroup by s_name\n"
            + "order by s_name";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt5", "l_partkey"), "l_partkey"),
                new AliasedColumn(
                    new BaseColumn("vt5", "l_suppkey"), "l_suppkey"),
                new AliasedColumn(
                    new ColumnOp(
                        "multiply",
                        Arrays.<UnnamedColumn>asList(
                            ConstantColumn.valueOf(0.5),
                            new ColumnOp(
                                "sum", new BaseColumn("vt5", "l_quantity")))),
                    "sum_quantity"),
                new AliasedColumn(
                    new BaseColumn("vt5", "l_partkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "0"),
                new AliasedColumn(
                    new BaseColumn("vt5", "l_suppkey"), 
                    AsyncAggExecutionNode.getGroupByAlias() + "1")),
            new BaseTable("tpch", "lineitem", "vt5"));
    expected.addFilterByAnd(
        new ColumnOp(
            "greaterequal",
            Arrays.asList(
                new BaseColumn("vt5", "l_shipdate"),
                ConstantColumn.valueOf("'1994-01-01'"))));
    expected.addFilterByAnd(
        new ColumnOp(
            "less",
            Arrays.asList(
                new BaseColumn("vt5", "l_shipdate"),
                ConstantColumn.valueOf("'1995-01-01'"))));
    expected.addGroupby(new BaseColumn("vt5", "l_partkey"));
    expected.addGroupby(new BaseColumn("vt5", "l_suppkey"));
    expected.setAliasName("vt4");
    assertEquals(
        expected,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);
    assertEquals(
        new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt4"),
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .getSelectQuery()
            .getFromList()
            .get(3));
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query21Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select\n"
            + "\ts_name,\n"
            + "\tcount(1) as numwait\n"
            + "from (\n"
            + "\tselect s_name from (\n"
            + "\t\tselect\n"
            + "\t\t\ts_name,\n"
            + "\t\t\tt2.l_orderkey,\n"
            + "\t\t\tl_suppkey,\n"
            + "\t\t\tcount_suppkey,\n"
            + "\t\t\tmax_suppkey\n"
            + "\t\tfrom\n"
            + "\t\t\t(select\n"
            + "\tl_orderkey,\n"
            + "\tcount(l_suppkey) count_suppkey,\n"
            + "\tmax(l_suppkey) as max_suppkey\n"
            + "from\n"
            + "\tlineitem\n"
            + "where\n"
            + "\tl_receiptdate > l_commitdate\n"
            + "\tand l_orderkey is not null\n"
            + "group by\n"
            + "\tl_orderkey) as t2 right outer join (\n"
            + "\t\t\tselect\n"
            + "\t\t\t\ts_name,\n"
            + "\t\t\t\tl_orderkey,\n"
            + "\t\t\t\tl_suppkey from (\n"
            + "\t\t\t\tselect\n"
            + "\t\t\t\t\ts_name,\n"
            + "\t\t\t\t\tt1.l_orderkey,\n"
            + "\t\t\t\t\tl_suppkey,\n"
            + "\t\t\t\t\tcount_suppkey,\n"
            + "\t\t\t\t\tmax_suppkey\n"
            + "\t\t\t\tfrom\n"
            + "\t\t\t\t\t(select\n"
            + "\tl_orderkey,\n"
            + "\tcount(l_suppkey) as count_suppkey,\n"
            + "\tmax(l_suppkey) as max_suppkey\n"
            + "from\n"
            + "\tlineitem\n"
            + "where\n"
            + "\tl_orderkey is not null\n"
            + "group by\n"
            + "\tl_orderkey) as t1 join (\n"
            + "\t\t\t\t\t\tselect\n"
            + "\t\t\t\t\t\t\ts_name,\n"
            + "\t\t\t\t\t\t\tl_orderkey,\n"
            + "\t\t\t\t\t\t\tl_suppkey\n"
            + "\t\t\t\t\t\tfrom\n"
            + "\t\t\t\t\t\t\torders o join (\n"
            + "\t\t\t\t\t\t\tselect\n"
            + "\t\t\t\t\t\t\t\ts_name,\n"
            + "\t\t\t\t\t\t\t\tl_orderkey,\n"
            + "\t\t\t\t\t\t\t\tl_suppkey\n"
            + "\t\t\t\t\t\t\tfrom\n"
            + "\t\t\t\t\t\t\t\tnation n join supplier s\n"
            + "\t\t\t\t\t\t\ton\n"
            + "\t\t\t\t\t\t\t\ts.s_nationkey = n.n_nationkey\n"
            + "\t\t\t\t\t\t\t\tand n.n_name = 'SAUDI ARABIA'\n"
            + "\t\t\t\t\t\t\t\tjoin lineitem l\n"
            + "\t\t\t\t\t\t\ton\n"
            + "\t\t\t\t\t\t\t\ts.s_suppkey = l.l_suppkey\n"
            + "\t\t\t\t\t\t\twhere\n"
            + "\t\t\t\t\t\t\t\tl.l_receiptdate > l.l_commitdate\n"
            + "\t\t\t\t\t\t\t\tand l.l_orderkey is not null\n"
            + "\t\t\t\t\t\t) l1 on o.o_orderkey = l1.l_orderkey and o.o_orderstatus = 'F'\n"
            + "\t\t\t\t\t) l2 on l2.l_orderkey = t1.l_orderkey\n"
            + "\t\t\t\t) a\n"
            + "\t\t\twhere\n"
            + "\t\t\t\t(count_suppkey > 1)\n"
            + "\t\t\t\tor ((count_suppkey=1)\n"
            + "\t\t\t\tand (l_suppkey <> max_suppkey))\n"
            + "\t\t) l3 on l3.l_orderkey = t2.l_orderkey\n"
            + "\t) b\n"
            + "\twhere\n"
            + "\t\t(count_suppkey is null)\n"
            + "\t\tor ((count_suppkey=1)\n"
            + "\t\tand (l_suppkey = max_suppkey))\n"
            + ") c\n"
            + "group by\n"
            + "\ts_name\n"
            + "order by\n"
            + "\tnumwait desc,\n"
            + "\ts_name \n"
            + "limit 100;";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        2,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        0,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(1)
            .getExecutableNodeBaseDependents()
            .size());

    assertEquals(
        new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt1"),
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    assertEquals(
        new BaseTable("placeholderSchema_2_0", "placeholderTable_2_0", "vt2"),
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    JoinTable join =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(
                new BaseTable("placeholderSchema_3_0", "placeholderTable_3_0", "vt3"),
                new BaseTable("placeholderSchema_3_1", "placeholderTable_3_1", "vt5")),
            Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.rightouter),
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "equal",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt5", "l_orderkey"),
                        new BaseColumn("vt3", "l_orderkey")))));
    assertEquals(
        join,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void Query22Test() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select\n"
            + "\tcntrycode,\n"
            + "\tcount(1) as numcust,\n"
            + "\tsum(c_acctbal) as totacctbal\n"
            + "from (\n"
            + "\tselect\n"
            + "\t\tcntrycode,\n"
            + "\t\tc_acctbal,\n"
            + "\t\tavg_acctbal\n"
            + "\tfrom\n"
            + "\t\t(select\n"
            + "\tavg(c_acctbal) as avg_acctbal\n"
            + "from\n"
            + "\t(select\n"
            + "\tc_acctbal,\n"
            + "\tc_custkey,\n"
            + "\tsubstr(c_phone, 1, 2) as cntrycode\n"
            + "from\n"
            + "\tcustomer\n"
            + "where\n"
            + "\tsubstr(c_phone, 1, 2) = '13' or\n"
            + "\tsubstr(c_phone, 1, 2) = '31' or\n"
            + "\tsubstr(c_phone, 1, 2) = '23' or\n"
            + "\tsubstr(c_phone, 1, 2) = '29' or\n"
            + "\tsubstr(c_phone, 1, 2) = '30' or\n"
            + "\tsubstr(c_phone, 1, 2) = '18' or\n"
            + "\tsubstr(c_phone, 1, 2) = '17')\n"
            + "where\n"
            + "\tc_acctbal > 0.00) as ct1 join (\n"
            + "\t\t\tselect\n"
            + "\t\t\t\tcntrycode,\n"
            + "\t\t\t\tc_acctbal\n"
            + "\t\t\tfrom\n"
            + "\t\t\t\t(select\n"
            + "\to_custkey\n"
            + "from\n"
            + "\torders\n"
            + "group by\n"
            + "\to_custkey) ot\n"
            + "\t\t\t\tright outer join (select\n"
            + "\tc_acctbal,\n"
            + "\tc_custkey,\n"
            + "\tsubstr(c_phone, 1, 2) as cntrycode\n"
            + "from\n"
            + "\tcustomer\n"
            + "where\n"
            + "\tsubstr(c_phone, 1, 2) = '13' or\n"
            + "\tsubstr(c_phone, 1, 2) = '31' or\n"
            + "\tsubstr(c_phone, 1, 2) = '23' or\n"
            + "\tsubstr(c_phone, 1, 2) = '29' or\n"
            + "\tsubstr(c_phone, 1, 2) = '30' or\n"
            + "\tsubstr(c_phone, 1, 2) = '18' or\n"
            + "\tsubstr(c_phone, 1, 2) = '17') ct\n"
            + "\t\t\t\ton ct.c_custkey = ot.o_custkey\n"
            + "\t\t\twhere\n"
            + "\t\t\t\to_custkey is null\n"
            + "\t\t) as ct2 on ct1.avg_acctbal > 0\n"
            + ") a\n"
            + "where\n"
            + "\tc_acctbal > avg_acctbal\n"
            + "group by\n"
            + "\tcntrycode\n"
            + "order by\n"
            + "\tcntrycode";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        2,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        1,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .size());
    assertEquals(
        2,
        queryExecutionPlan
            .root
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(0)
            .getExecutableNodeBaseDependents()
            .get(1)
            .getExecutableNodeBaseDependents()
            .size());

    assertEquals(
        new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", "vt1"),
        ((CreateTableAsSelectNode) queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    JoinTable join =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(
                new BaseTable("placeholderSchema_2_0", "placeholderTable_2_0", "vt2"),
                new BaseTable("placeholderSchema_2_1", "placeholderTable_2_1", "vt5")),
            Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.inner),
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "greater",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt2", "avg_acctbal"), ConstantColumn.valueOf(0)))));
    assertEquals(
        join,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    assertEquals(
        new BaseTable("placeholderSchema_3_0", "placeholderTable_3_0", "vt3"),
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .getSelectQuery()
            .getFromList()
            .get(0));
    JoinTable join1 =
        JoinTable.create(
            Arrays.<AbstractRelation>asList(
                new BaseTable("placeholderSchema_5_0", "placeholderTable_5_0", "vt6"),
                new BaseTable("placeholderSchema_5_1", "placeholderTable_5_1", "vt8")),
            Arrays.<JoinTable.JoinType>asList(JoinTable.JoinType.rightouter),
            Arrays.<UnnamedColumn>asList(
                new ColumnOp(
                    "equal",
                    Arrays.<UnnamedColumn>asList(
                        new BaseColumn("vt8", "c_custkey"),
                        new BaseColumn("vt6", "o_custkey")))));
    assertEquals(
        join1,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(1))
            .getSelectQuery()
            .getFromList()
            .get(0));

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void SubqueryInFilterTest() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select avg(l_quantity) from lineitem where l_quantity > (select avg(l_quantity) as quantity_avg from lineitem);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    //    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create(new
    // JdbcConnection(conn, new H2Syntax()),
    //        new H2Syntax(), meta, (SelectQuery) relation, "verdictdb_temp");
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    String aliasName = String.format("verdictdb_alias_%d_2", queryExecutionPlan.getSerialNumber());
    SelectQuery rewritten =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("placeholderSchema_1_0", aliasName, "quantity_avg"),
                    "quantity_avg")),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", aliasName));
    assertEquals(
        rewritten,
        ((SubqueryColumn)
                ((ColumnOp)
                        ((CreateTableAsSelectNode)
                                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
                            .getSelectQuery()
                            .getFilter()
                            .get())
                    .getOperand(1))
            .getSubquery());

    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new ColumnOp("avg", new BaseColumn("vt3", "l_quantity")),
                    "quantity_avg")),
            new BaseTable("tpch", "lineitem", "vt3"));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void SubqueryInFilterTestExists() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(l_quantity) from lineitem where exists (select * from lineitem);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    String alias =
        ((CreateTableAsSelectNode)
                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
            .getPlaceholderTables()
            .get(0)
            .getAliasName()
            .get();
    SelectQuery rewritten =
        SelectQuery.create(
            Arrays.<SelectItem>asList(new AsteriskColumn()),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", alias));
    assertEquals(
        rewritten,
        ((SubqueryColumn)
                ((ColumnOp)
                        ((CreateTableAsSelectNode)
                                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
                            .getSelectQuery()
                            .getFilter()
                            .get())
                    .getOperand(0))
            .getSubquery());

    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(new AsteriskColumn()),
            new BaseTable("tpch", "lineitem", "vt3"));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void SubqueryInFilterTestIn() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select avg(l_quantity) from lineitem where l_quantity in (select distinct l_quantity from lineitem);";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    String alias =
        ((CreateTableAsSelectNode)
                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
            .getPlaceholderTables()
            .get(0)
            .getAliasName()
            .get();
    SelectQuery rewritten =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("placeholderSchema_1_0", alias, "l_quantity"), "l_quantity")),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", alias));
    assertEquals(
        rewritten,
        ((SubqueryColumn)
                ((ColumnOp)
                        ((CreateTableAsSelectNode)
                                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
                            .getSelectQuery()
                            .getFilter()
                            .get())
                    .getOperand(1))
            .getSubquery());

    SelectQuery expected =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("vt3", "l_quantity"), "l_quantity")),
            new BaseTable("tpch", "lineitem", "vt3"));
    assertEquals(
        expected,
        ((CreateTableAsSelectNode)
                queryExecutionPlan
                    .root
                    .getExecutableNodeBaseDependents()
                    .get(0)
                    .getExecutableNodeBaseDependents()
                    .get(0))
            .selectQuery);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void SubqueryInFilterMultiplePredicateTest() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql =
        "select avg(l_quantity) from lineitem where l_quantity > (select avg(l_quantity) as quantity_avg from lineitem) and l_quantity in (select distinct l_quantity from lineitem)";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);
    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();

    String alias =
        ((CreateTableAsSelectNode)
                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
            .getPlaceholderTables()
            .get(0)
            .getAliasName()
            .get();
    SelectQuery rewritten1 =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("placeholderSchema_1_0", alias, "quantity_avg"),
                    "quantity_avg")),
            new BaseTable("placeholderSchema_1_0", "placeholderTable_1_0", alias));
    assertEquals(
        rewritten1,
        ((SubqueryColumn)
                ((ColumnOp)
                        ((ColumnOp)
                                ((QueryNodeBase)
                                        queryExecutionPlan
                                            .root
                                            .getExecutableNodeBaseDependents()
                                            .get(0))
                                    .getSelectQuery()
                                    .getFilter()
                                    .get())
                            .getOperand(0))
                    .getOperand(1))
            .getSubquery());

    alias =
        ((CreateTableAsSelectNode)
                (queryExecutionPlan.root.getExecutableNodeBaseDependents().get(0)))
            .getPlaceholderTables()
            .get(1)
            .getAliasName()
            .get();
    SelectQuery rewritten2 =
        SelectQuery.create(
            Arrays.<SelectItem>asList(
                new AliasedColumn(
                    new BaseColumn("placeholderSchema_1_1", alias, "l_quantity"), "l_quantity")),
            new BaseTable("placeholderSchema_1_1", "placeholderTable_1_1", alias));
    assertEquals(
        rewritten2,
        ((SubqueryColumn)
                ((ColumnOp)
                        ((ColumnOp)
                                ((QueryNodeBase)
                                        queryExecutionPlan
                                            .root
                                            .getExecutableNodeBaseDependents()
                                            .get(0))
                                    .getSelectQuery()
                                    .getFilter()
                                    .get())
                            .getOperand(1))
                    .getOperand(1))
            .getSubquery());

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new
    // H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }
}
