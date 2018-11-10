package org.verdictdb.core.querying.ola;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanFactory;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlwriter.QueryToSql;
import org.verdictdb.sqlwriter.SelectQueryToSql;

public class AsyncAggScaleTest {

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 2;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;

  String placeholderSchemaName = "placeholderSchemaName";

  String placeholderTableName = "placeholderTableName";

  static String originalSchema = "originalSchema";

  static String originalTable = "originalTable";

  static String smallTable = "smallTable";

  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggscaletest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    stmt.executeUpdate(
        String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)",
            originalSchema, originalTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          originalSchema, originalTable, i, (double) i+1));
    }
    stmt.executeUpdate(
        String.format("CREATE TABLE \"%s\".\"%s\"(\"s_id\" int, \"s_value\" double)",
            originalSchema, smallTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(
          String.format("INSERT INTO \"%s\".\"%s\"(\"s_id\", \"s_value\") VALUES(%s, %f)",
              originalSchema, smallTable, i, (double) i+1));
    }

    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, originalSchema,
            "originalTable_scrambled", aggBlockCount);
    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.5, 1.0));
    tablemeta.setCumulativeDistributionForTier(distribution);
    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
        new ImmutablePair<>("value", DOUBLE)
    ));
    staticMetaData.addTableData(
        new StaticMetaData.TableInfo(originalSchema, "originalTable_scrambled"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
        new ImmutablePair<>("s_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, smallTable), arr);
  }

  @Test
  public void ScrambleTableTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
//<<<<<<< HEAD:src/test/java/org/verdictdb/core/querying/AsyncAggScaleTest.java
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)), 
        (queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);
//    queryExecutionPlan.setScalingNode();
//=======
//    Assert.assertEquals(new HyperTableCube(Arrays.asList(d1)), ((AggExecutionNode)queryExecutionPlan.getRootNode().getDependents().get(0).getDependents().get(0)).getCubes().get(0));
//    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getDependents().get(0)).setScrambleMetaSet(meta);
    //>>>>>>> origin/joezhong-scale:src/test/java/org/verdictdb/core/querying/ola/AsyncAggScaleTest.java
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
//    queryExecutionPlan.getRoot().print();
    
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    queryExecutionPlan.root.executeAndWaitForTermination();
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

/*
  @Test
  public void ScrambleTableCompressTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);
//    queryExecutionPlan.setScalingNode();
    QueryExecutionPlanSimplifier.simplify(queryExecutionPlan);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }
*/

  @Test
  public void ScrambleTableAvgTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select (1+avg(value))*sum(value), count(*), count(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        (queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  /*
  @Test
  public void toSqlTest() throws VerdictDBException,SQLException {
    String sql = "select (1+avg(value))*sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan =
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("vt\\d+", "vt");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = "select sum(vt.\"value\") as \"agg0\", "
        + "count(*) as \"agg1\", vt.\"verdictdbtier\" as \"verdictdb_tier_alias\" "
        + "from \"originalSchema\".\"originalTable_scrambled\" as vt "
        + "where vt.\"verdictdbaggblock\" = 0 "
        + "group by vt.\"verdictdbtier\"";
    assertEquals(expected, actual);
  
    ExecutableNodeBase combiner = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combiner.getId()*1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combiner.getId()*1000 + 1);
    query = (CreateTableAsSelectQuery) combiner.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("vt\\d+", "vt");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
                   "sum(unionTable.\"agg0\") as \"agg0\", " +
                   "sum(unionTable.\"agg1\") as \"agg1\", " +
                   "unionTable.\"verdictdb_tier_alias\" as \"verdictdb_tier_alias\" " +
                   "from (" +
                   "select * from \"verdict_temp\".\"table2\" as verdictdb_alias " +
                   "UNION ALL " +
                   "select * from \"verdict_temp\".\"table1\" as verdictdb_alias) as unionTable " +
                   "group by unionTable.\"verdictdb_tier_alias\"";
    assertEquals(expected, actual);
  
    ExecutableNodeBase aggNode = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0);
    ExecutableNodeBase asyncNode = queryExecutionPlan.getRoot().getSources().get(0);
    ExecutionInfoToken token3 = aggNode.createToken(null);
    token3.setKeyValue("channel", asyncNode.getId()*1000);
    query = (CreateTableAsSelectQuery) asyncNode.createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("vt\\d+", "vt");
    actual = actual.replaceAll("vc\\d+", "vc");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    expected = "select (1 + (sum(verdictdb_internal_tier_consolidated.\"agg0\") / " +
                   "sum(verdictdb_internal_tier_consolidated.\"agg1\"))) * " +
                   "sum(verdictdb_internal_tier_consolidated.\"agg0\") as \"vc\" " +
                   "from (" +
                   "select (case when (verdictdb_internal_before_scaling.\"verdictdb_tier_alias\" = 0) then 2.0 else 1.0 end)" +
                   " * verdictdb_internal_before_scaling.\"agg0\" as \"agg0\", " +
                   "(case when (verdictdb_internal_before_scaling.\"verdictdb_tier_alias\" = 0) then 2.0 else 1.0 end)" +
                   " * verdictdb_internal_before_scaling.\"agg1\" as \"agg1\", " +
                   "verdictdb_internal_before_scaling.\"verdictdb_tier_alias\" as \"verdictdb_tier_alias\" " +
                   "from \"verdictdb_temp\".\"verdictdbtemptable\" as verdictdb_internal_before_scaling) " +
                   "as verdictdb_internal_tier_consolidated";
    assertEquals(expected, actual);
  }
  */
  @Test
  public void simpleAggTest1() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void simpleAggTest2() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void simpleAggTest3() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void simpleAggTest4() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value), sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void simpleAggTest5() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value), count(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void simpleAggTest6() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value), sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void maxAggTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select (1+max(value))*avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void minAggTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select (1+min(value))*avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ( queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((SelectAsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

}
