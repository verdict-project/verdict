package org.verdictdb.core.querying.ola;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static org.junit.Assert.assertEquals;

import java.math.BigDecimal;
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
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcDbmsConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.resulthandler.ExecutionResultReader;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.H2Syntax;

public class AsyncAggScaleResultTest {

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 5;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;


  static String originalSchema = "originalSchema";

  static String originalTable = "originalTable";


  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggscaletest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"value\" float, \"verdicttier\" int, \"verdictdbaggblock\" int)", originalSchema, originalTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"value\", \"verdicttier\", \"verdictdbaggblock\") VALUES(%f, %s, %s)",
          originalSchema, originalTable, (float)1, 0, i/2));
    }

    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable", aggBlockCount);
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.2, 0.4, 0.6, 0.8, 1.0));
    tablemeta.setCumulativeMassDistributionPerTier(distribution);
    scrambledTable = tablemeta.getTableName();
    meta.insertScrambleMetaEntry(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("value", DOUBLE),
        new ImmutablePair<>("verdicttier", BIGINT),
        new ImmutablePair<>("verdictdbaggblock", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable"), arr);

    stmt.execute("create schema if not exists \"verdictdb_temp\";");
  }

  @Test
  public void simpleAggTest1() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(1.0, (double)dbmsQueryResult.getValue(0), 1e-6);
    }
  }

  @Test
  public void simpleAggTest2() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(10, ((BigDecimal)dbmsQueryResult.getValue(0)).longValue());
    }
  }

  @Test
  public void simpleAggTest3() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(10, (double)dbmsQueryResult.getValue(0), 1e-6);
    }
  }

  @Test
  public void simpleAggTest4() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value), sum(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(10, ((BigDecimal)dbmsQueryResult.getValue(0)).longValue());
      assertEquals(10, (double)dbmsQueryResult.getValue(1), 1e-6);
    }
  }

  @Test
  public void simpleAggTest5() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(value), avg(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(10, ((BigDecimal)dbmsQueryResult.getValue(0)).longValue());
      assertEquals(1.0, (double)dbmsQueryResult.getValue(1), 1e-6);
    }
  }

  @Test
  public void simpleAggTest6() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value), avg(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(10, (double)dbmsQueryResult.getValue(0), 1e-6);
      assertEquals(1.0, (double)dbmsQueryResult.getValue(1), 1e-6);
    }
  }

  @Test
  public void maxAggTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select max(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(1.0, (double)dbmsQueryResult.getValue(0), 1e-6);
    }
  }

  @Test
  public void minAggTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select min(value) from originalTable";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = new QueryExecutionPlan("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension("originalSchema", "originalTable", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMeta(meta);


    JdbcDbmsConnection jdbcConnection = new JdbcDbmsConnection(conn, new H2Syntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      assertEquals(1.0, (double)dbmsQueryResult.getValue(0), 1e-6);
    }
  }


  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
  }
}
