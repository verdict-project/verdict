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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.QueryExecutionPlan;
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

public class AsyncAggMultipleTiersScaleTest {

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
    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggmultipletiersscaletest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", originalSchema, originalTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          originalSchema, originalTable, i, (double) i+1));
    }
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"s_id\" int, \"s_value\" double)", originalSchema, smallTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"s_id\", \"s_value\") VALUES(%s, %f)",
          originalSchema, smallTable, i, (double) i+1));
    }

    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable_scrambled", aggBlockCount);
    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(2);
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.5, 1.0));
    distribution.put(1, Arrays.asList(0.2, 1.0));
    tablemeta.setCumulativeDistributionForTier(distribution);
    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
        new ImmutablePair<>("value", DOUBLE),
        new ImmutablePair<>("verdictdbtier", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable_scrambled"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
        new ImmutablePair<>("s_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, smallTable), arr);
    
    // scratchpad schema
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("drop schema if exists \"verdictdb_temp\" cascade;");
  }

  @Test
  public void ScrambleTableTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan =
        new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)), 
        ((AggExecutionNode)queryExecutionPlan.getRootNode().getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0)).getMeta().getCubes().get(0));

    ((AsyncAggExecutionNode) queryExecutionPlan.getRoot().getExecutableNodeBaseDependent(0))
    .setScrambleMeta(meta);
    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    //queryExecutionPlan.getRoot().executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }

  @Test
  public void toSqlTest() throws VerdictDBException,SQLException {
    String sql = "select (1+avg(value))*sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select sum(vt3.\"value\") as \"agg0\", "
        + "count(*) as \"agg1\", vt3.\"verdictdbtier\" as \"verdictdbtier0\" "
        + "from \"originalSchema\".\"originalTable_scrambled\" as vt3 "
        + "where vt3.\"verdictdbaggblock\" = 0 "
        + "group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select (1 + (sum(verdictdbafterscaling.\"agg0\") / sum(verdictdbafterscaling.\"agg1\"))) * sum(verdictdbafterscaling.\"agg0\") as \"vc4\" " +
        "from " +
        "(select case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(actual, expected);
  }

  @Test
  public void simpleAggTest1() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select sum(vt1.\"value\") as \"agg0\", " +
        "count(*) as \"agg1\", vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" " +
        "as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 " +
        "group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select sum(verdictdbafterscaling.\"agg0\") / sum(verdictdbafterscaling.\"agg1\") as \"a2\" " +
        "from (select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void simpleAggTest2() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "sum(vt1.\"value\") as \"agg0\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 " +
        "group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(verdictdbafterscaling.\"agg0\") as \"s2\" " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void simpleAggTest3() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");


    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "count(*) as \"agg0\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 " +
        "group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(verdictdbafterscaling.\"agg0\") as \"c2\" " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void simpleAggTest4() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value), sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "sum(vt1.\"value\") as \"agg0\", " +
        "count(*) as \"agg1\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" " +
        "as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 " +
        "group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(verdictdbafterscaling.\"agg0\") / sum(verdictdbafterscaling.\"agg1\") as \"a2\", " +
        "sum(verdictdbafterscaling.\"agg0\") as \"s3\" " +
        "from " +
        "(select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void simpleAggTest5() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value), count(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "sum(vt1.\"value\") as \"agg0\", " +
        "count(*) as \"agg1\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" " +
        "as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 " +
        "group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(verdictdbafterscaling.\"agg0\") / sum(verdictdbafterscaling.\"agg1\") as \"a2\", " +
        "sum(verdictdbafterscaling.\"agg1\") as \"c3\" " +
        "from " +
        "(select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void simpleAggTest6() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value), sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
    
    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "count(*) as \"agg0\", " +
        "sum(vt1.\"value\") as \"agg1\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(verdictdbafterscaling.\"agg0\") as \"c2\", " +
        "sum(verdictdbafterscaling.\"agg1\") as \"s3\" " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void maxAggTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select (1+max(value))*avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "sum(vt1.\"value\") as \"agg0\", " +
        "count(*) as \"agg1\", " +
        "max(vt1.\"value\") as \"agg2\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "max(unionTable.\"agg2\") as \"agg2\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "(1 + max(verdictdbafterscaling.\"agg2\")) * " + 
        "(sum(verdictdbafterscaling.\"agg0\") / sum(verdictdbafterscaling.\"agg1\")) as \"vc2\" " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"agg2\" as \"agg2\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }

  @Test
  public void minAggTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select (1+min(value))*avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);

//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new H2Syntax());
    String actual = queryToSql.toSql(query.getSelect());
    String expected = "select " +
        "sum(vt1.\"value\") as \"agg0\", " +
        "count(*) as \"agg1\", " +
        "min(vt1.\"value\") as \"agg2\", " +
        "vt1.\"verdictdbtier\" as \"verdictdbtier0\" " +
        "from \"originalSchema\".\"originalTable_scrambled\" as vt1 " +
        "where vt1.\"verdictdbaggblock\" = 0 group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbalias_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "sum(unionTable.\"agg0\") as \"agg0\", " +
        "sum(unionTable.\"agg1\") as \"agg1\", " +
        "min(unionTable.\"agg2\") as \"agg2\", " +
        "unionTable.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from (" +
        "select * from \"verdict_temp\".\"table1\" as alias " +
        "UNION ALL " +
        "select * from \"verdict_temp\".\"table2\" as alias) " +
        "as unionTable group by \"verdictdbtier0\"";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select " +
        "(1 + min(verdictdbafterscaling.\"agg2\")) * " +
        "(sum(verdictdbafterscaling.\"agg0\") / sum(verdictdbafterscaling.\"agg1\")) as \"vc2\" " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg0\") " +
        "else 0 end as \"agg0\", " +
        "case " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 1) then (5.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "when (verdictdbbeforescaling.\"verdictdbtier0\" = 0) then (2.0000000000000000 * verdictdbbeforescaling.\"agg1\") " +
        "else 0 end as \"agg1\", " +
        "verdictdbbeforescaling.\"agg2\" as \"agg2\", " +
        "verdictdbbeforescaling.\"verdictdbtier0\" as \"verdictdbtier0\" " +
        "from \"verdictdb_temp\".\"alias\" as verdictdbbeforescaling) " +
        "as verdictdbafterscaling";
    assertEquals(expected, actual);
  }
}
