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
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
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
import org.verdictdb.sqlsyntax.MysqlSyntax;

public class AsyncAggScaleResultMultiTierTest {

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 5;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();

  static String scrambledTable;

  static String originalSchema = "originalSchema";

  static String originalTable = "originalTable";

  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "test";

  private static final String MYSQL_UESR = "root";

  private static final String MYSQL_PASSWORD = "";

  private static final String TABLE_NAME = "mytable";

  static {
    String env = System.getenv("BUILD_ENV");
    if (env != null && (env.equals("GitLab") || env.equals("DockerCompose"))) {
      MYSQL_HOST = "mysql";
    } else {
      MYSQL_HOST = "localhost";
    }
  }


  @BeforeClass
  public static void setupMysqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
    
    stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", originalSchema));
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (`value` float, `verdictdbtier` int, `verdictdbaggblock` int)", originalSchema, originalTable));
    for (int i = 0; i < 15; i++) {
      stmt.executeUpdate(String.format("INSERT INTO `%s`.`%s` (`value`, `verdictdbtier`, `verdictdbaggblock`) VALUES (%f, %s, %s)",
          originalSchema, originalTable, (float)1, i%3==0?0:1, i/3));
    }

    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable", aggBlockCount);
    ScrambleMeta tablemeta = scrambler.generateMeta();
    tablemeta.setNumberOfTiers(2);
    HashMap<Integer, List<Double>> distribution = new HashMap<>();
    distribution.put(0, Arrays.asList(0.2, 0.4, 0.6, 0.8, 1.0));
    distribution.put(1, Arrays.asList(0.2, 0.4, 0.6, 0.8, 1.0));
    tablemeta.setCumulativeDistributionForTier(distribution);
    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);
    
    // create scrambled table
//    DbmsConnection dbmsConn = JdbcConnection.create(conn);
//    String scrambleSchema = MYSQL_DATABASE;
//    String scratchpadSchema = MYSQL_DATABASE;
//    String scrambledTable = "originalTable_scrambled";
//    String primaryColumn = null;
//    Map<String, String> options = new HashMap<>();
//    options.put("blockColumnName", "verdictdbaggblock");
//    options.put("tierColumnName", "verdictdbtier");
//    long blockSize = 5;
//    ScramblingCoordinator scrambler = new ScramblingCoordinator(dbmsConn, scrambleSchema, scratchpadSchema, blockSize);
//    ScrambleMeta tablemeta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "fastconverge", primaryColumn, options);
//    meta.addScrambleMeta(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("value", DOUBLE),
        new ImmutablePair<>("verdictdbtier", BIGINT),
        new ImmutablePair<>("verdictdbaggblock", BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable"), arr);

    stmt.execute("create schema if not exists `verdictdb_temp`");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", originalSchema));
    stmt.execute("drop schema `verdictdb_temp`");
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(1.0, (double)dbmsQueryResult.getValue(0), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(15, ((BigDecimal)dbmsQueryResult.getValue(0)).longValue());
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(15, (double)dbmsQueryResult.getValue(0), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(15, ((BigDecimal)dbmsQueryResult.getValue(0)).longValue());
      assertEquals(15, (double)dbmsQueryResult.getValue(1), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(15, ((BigDecimal)dbmsQueryResult.getValue(0)).longValue());
      assertEquals(1.0, (double)dbmsQueryResult.getValue(1), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(15, (double)dbmsQueryResult.getValue(0), 1e-6);
      assertEquals(1.0, (double)dbmsQueryResult.getValue(1), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(1.0, dbmsQueryResult.getDouble(0), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
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


    JdbcConnection jdbcConnection = new JdbcConnection(conn, new MysqlSyntax());
    //ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);

    ExecutionResultReader reader = ExecutablePlanRunner.getResultReader(jdbcConnection, queryExecutionPlan);
    int resultReturnedCnt = 0;
    while (reader.hasNext()) {
      DbmsQueryResult dbmsQueryResult = reader.next();
      dbmsQueryResult.next();
      resultReturnedCnt++;
      assertEquals(1.0, dbmsQueryResult.getDouble(0), 1e-6);
    }
    assertEquals(5, resultReturnedCnt);
  }
}
