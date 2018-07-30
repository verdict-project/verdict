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
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.connection.StaticMetaData;
import org.verdictdb.coordinator.ScramblingCoordinator;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.QueryExecutionPlanFactory;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.NonValidatingSQLParser;
import org.verdictdb.sqlreader.RelationStandardizer;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlwriter.SelectQueryToSql;

public class AsyncAggMultipleTiersScaleTest {
  
  private static final String MYSQL_HOST;

  private static final String MYSQL_DATABASE = "async_agg_multi_tier_scale_test";

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

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 2;

  static ScrambleMetaSet meta = new ScrambleMetaSet();

  static StaticMetaData staticMetaData = new StaticMetaData();

//  static String scrambledTable;

  String placeholderSchemaName = "placeholderSchemaName";

  String placeholderTableName = "placeholderTableName";

//  static String originalSchema = "originalSchema";
  static String originalSchema = MYSQL_DATABASE;

  static String originalTable = "originalTable";

  static String smallTable = "smallTable";
  
  final static String tierColumn = "mytier";
  
  @BeforeClass
  public static void setupMySqlDatabase() throws SQLException, VerdictDBException {
    String mysqlConnectionString =
        String.format("jdbc:mysql://%s?autoReconnect=true&useSSL=false", MYSQL_HOST);
    conn = DriverManager.getConnection(mysqlConnectionString, MYSQL_UESR, MYSQL_PASSWORD);
//    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggmultipletiersscaletest;DB_CLOSE_DELAY=-1";
//    final String DB_USER = "";
//    final String DB_PASSWORD = "";
//    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);

    stmt = conn.createStatement();
    stmt.execute(String.format("DROP SCHEMA IF EXISTS `%s`", originalSchema));
    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS `%s`", originalSchema));
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (`id` int, `value` double)", originalSchema, originalTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO `%s`.`%s` (`id`, `value`) VALUES(%s, %f)",
          originalSchema, originalTable, i, (double) i+1));
    }
    stmt.executeUpdate(String.format("CREATE TABLE `%s`.`%s` (`s_id` int, `s_value` double)", originalSchema, smallTable));
    for (int i = 0; i < 10; i++) {
      stmt.executeUpdate(String.format("INSERT INTO `%s`.`%s` (`s_id`, `s_value`) VALUES(%s, %f)",
          originalSchema, smallTable, i, (double) i+1));
    }
    
    // create scrambled table
    DbmsConnection dbmsConn = JdbcConnection.create(conn);
    String scrambleSchema = MYSQL_DATABASE;
    String scratchpadSchema = MYSQL_DATABASE;
    String scrambledTable = "originalTable_scrambled";
    String primaryColumn = null;
    Map<String, String> options = new HashMap<>();
    options.put("blockColumnName", "verdictdbaggblock");
//    options.put("tierColumnName", "verdictdbtier");
    options.put("tierColumnName", tierColumn);
    long blockSize = 5;
    ScramblingCoordinator scrambler = new ScramblingCoordinator(dbmsConn, scrambleSchema, scratchpadSchema, blockSize);
    ScrambleMeta tablemeta = scrambler.scramble(originalSchema, originalTable, originalSchema, scrambledTable, "fastconverge", primaryColumn, options);
    ScrambleMeta tablemeta2 = scrambler.scramble(originalSchema, smallTable, originalSchema, "smallTable_scrambled", "fastconverge", primaryColumn, options);
//    UniformScrambler scrambler =
//        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable_scrambled", aggBlockCount);
//    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
//    stmt.executeUpdate(QueryToSql.convert(new MysqlSyntax(), scramblingQuery));
//    ScrambleMeta tablemeta = scrambler.generateMeta();
//    tablemeta.setNumberOfTiers(2);
//    HashMap<Integer, List<Double>> distribution = new HashMap<>();
//    distribution.put(0, Arrays.asList(0.5, 1.0));
//    distribution.put(1, Arrays.asList(0.2, 1.0));
//    tablemeta.setCumulativeDistributionForTier(distribution);
//    scrambledTable = tablemeta.getTableName();
    meta.addScrambleMeta(tablemeta);
    meta.addScrambleMeta(tablemeta2);
    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
        new ImmutablePair<>("value", DOUBLE),
        new ImmutablePair<>(tierColumn, BIGINT)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, scrambledTable), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
        new ImmutablePair<>("s_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "smallTable_scrambled"), arr);
    
    // scratchpad schema
    stmt.execute("create schema if not exists `verdictdb_temp`;");
  }

  @AfterClass
  public static void tearDown() throws SQLException {
    stmt.execute("drop schema if exists `verdictdb_temp`;");
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
        QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)), 
        ((AggExecutionNode)queryExecutionPlan.getRootNode().getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0)).getAggMeta().getCubes().get(0));

    ((AsyncAggExecutionNode) queryExecutionPlan.getRoot().getExecutableNodeBaseDependent(0))
    .setScrambleMetaSet(meta);
    stmt.execute("create schema if not exists `verdictdb_temp`;");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new MysqlSyntax()), queryExecutionPlan);
    //queryExecutionPlan.getRoot().executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    stmt.execute("drop schema `verdictdb_temp` cascade;");
  }
  
  @Test
  public void nestedAggTest1() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from (select value from originalTable_scrambled)";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    //jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("schemaName", originalSchema);
    token.setKeyValue("tableName", "originalTable_scrambled");
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select sum(vt1.`value`) as `agg0`, " +
        "count(*) as `agg1`, vt1.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` " +
        "as vt1 " +
        "group by `verdictdb_tier_alias`",
         originalSchema);
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`) as `a3` " +
        "from (select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
    assertEquals(expected, actual);
  }

  @Test
  public void nestedAggTest2() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from (select * from (select value from originalTable_scrambled))";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("schemaName", originalSchema);
    token.setKeyValue("tableName", "originalTable_scrambled");
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("`verdictdb_alias_\\d+_\\d+`", "`verdictdb_alias`");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select sum(vt1.`value`) as `agg0`, " +
            "count(*) as `agg1`, vt1.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
            "from `%s`.`originalTable_scrambled` " +
            "as vt1 " +
            "group by `verdictdb_tier_alias`",
        originalSchema);
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`) as `a4` " +
        "from (select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
    assertEquals(expected, actual);
  }

  @Test
  public void nestedAggTest3() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from (select * from (select value from originalTable_scrambled)) " +
                     "inner join (select s_value from smallTable_scrambled) on value=s_value";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
   // Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
   // assertEquals(
   //     new HyperTableCube(Arrays.asList(d1)),
   //     ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    //jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token1 = new ExecutionInfoToken(), token2 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", originalSchema);
    token1.setKeyValue("tableName", "originalTable_scrambled");
    token2.setKeyValue("schemaName", originalSchema);
    token2.setKeyValue("tableName", "smallTable_scrambled");
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token1, token2));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format(
        "select sum(vt1.`value`) as `agg0`, " +
            "vt1.`verdictdb_tier_alias` as `verdictdb_tier_alias`, " +
            "vt4.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
            "from `%s`.`originalTable_scrambled` " +
            "as vt1 inner join `%s`.`smallTable_scrambled` as vt4 on " +
            "(vt1.`value` = vt4.`s_value`) " +
            "group by `verdictdb_tier_alias`, `verdictdb_tier_alias`",
        originalSchema, originalSchema);
    assertEquals(expected, actual);

    token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable " +
        "group by `verdictdb_tier_alias`, `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    // non-deterministic
    /*
    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_[0-9]*_[0-9]", "alias");
    expected = "select sum(verdictdb_internal_tier_consolidated.`agg0`) as `s6` " +
        "from (select " +
        "case when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 0) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 1)) " +
        "then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 1) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 2)) " +
        "then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 0) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 0)) " +
        "then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 1) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 1)) " +
        "then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 2) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 2)) " +
        "then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 1) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 0)) " +
        "then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 2) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 1)) " +
        "then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 2) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 0)) " +
        "then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when ((verdictdb_internal_before_scaling.`verdictdb_tier_internal1` = 0) and (verdictdb_internal_before_scaling.`verdictdb_alias` = 2)) " +
        "then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "verdictdb_internal_before_scaling.`verdictdb_alias` as `verdictdb_alias`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_internal1` as `verdictdb_tier_internal1` " +
        "from `verdictdb_temp`.`alias2` as verdictdb_internal_before_scaling) as verdictdb_internal_tier_consolidated";
    assertEquals(expected, actual);
    */
  }

  @Test
  public void nestedAggTest4() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from (select 1+value as value from (select 2+value as value from originalTable_scrambled))";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("schemaName", originalSchema);
    token.setKeyValue("tableName", "originalTable_scrambled");

    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select sum(vt1.`value`) as `agg0`, " +
            "vt1.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
            "from `%s`.`originalTable_scrambled` " +
            "as vt1 " +
            "group by `verdictdb_tier_alias`",
        originalSchema);
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

  }

  @Test
  public void toSqlTest() throws VerdictDBException,SQLException {
    String sql = "select (1+avg(value))*sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    actual = actual.replaceAll("vt\\d", "vt1");
    String expected = String.format("select sum(vt1.`value`) as `agg0`, "
        + "count(*) as `agg1`, vt1.`%s` as `verdictdb_tier_alias` "
        + "from `%s`.`originalTable_scrambled` as vt1 "
        + "where vt1.`verdictdbaggblock` = 0 "
        + "group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    actual = actual.replaceAll("vc\\d+", "vc1");
    expected = "select (1 + (sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`))) * sum(verdictdb_internal_tier_consolidated.`agg0`) as `vc1` " +
        "from " +
        "(select case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
    assertEquals(actual, expected);
  }

  @Test
  public void simpleAggTest1() throws VerdictDBException, SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select avg(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select sum(vt1.`value`) as `agg0`, " +
        "count(*) as `agg1`, vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` " +
        "as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 " +
        "group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);
  
    ExecutableNodeBase combinerNode = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combinerNode.getId() * 1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combinerNode.getId() * 1000 + 1);
    query = (CreateTableAsSelectQuery) combinerNode.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`) as `a2` " +
        "from (select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "sum(vt1.`value`) as `agg0`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 " +
        "group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);
  
    ExecutableNodeBase combinerNode = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combinerNode.getId() * 1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combinerNode.getId() * 1000 + 1);
    query = (CreateTableAsSelectQuery) combinerNode.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(verdictdb_internal_tier_consolidated.`agg0`) as `s2` " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");


    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "count(*) as `agg0`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 " +
        "group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);
  
    ExecutableNodeBase combinerNode = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combinerNode.getId() * 1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combinerNode.getId() * 1000 + 1);
    query = (CreateTableAsSelectQuery) combinerNode.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(verdictdb_internal_tier_consolidated.`agg0`) as `c2` " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "sum(vt1.`value`) as `agg0`, " +
        "count(*) as `agg1`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` " +
        "as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 " +
        "group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);

    ExecutableNodeBase combiner = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combiner.getId() * 1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combiner.getId() * 1000 + 1);
    query = (CreateTableAsSelectQuery) combiner.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`) as `a2`, " +
        "sum(verdictdb_internal_tier_consolidated.`agg0`) as `s3` " +
        "from " +
        "(select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "sum(vt1.`value`) as `agg0`, " +
        "count(*) as `agg1`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` " +
        "as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 " +
        "group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);
  
    ExecutableNodeBase combiner = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combiner.getId() * 1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combiner.getId() * 1000 + 1);
    query = (CreateTableAsSelectQuery) combiner.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`) as `a2`, " +
        "sum(verdictdb_internal_tier_consolidated.`agg1`) as `c3` " +
        "from " +
        "(select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");
    
    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "count(*) as `agg0`, " +
        "sum(vt1.`value`) as `agg1`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);

    ExecutableNodeBase combiner = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1);
    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", combiner.getId() * 1000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", combiner.getId() * 1000 + 1);
    query = (CreateTableAsSelectQuery) combiner.createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(verdictdb_internal_tier_consolidated.`agg0`) as `c2`, " +
        "sum(verdictdb_internal_tier_consolidated.`agg1`) as `s3` " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "sum(vt1.`value`) as `agg0`, " +
        "count(*) as `agg1`, " +
        "max(vt1.`value`) as `agg2`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "max(unionTable.`agg2`) as `agg2`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "(1 + max(verdictdb_internal_tier_consolidated.`agg2`)) * " + 
        "(sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`)) as `vc2` " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`agg2` as `agg2`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
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

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
//    Dimension d1 = new Dimension("originalSchema", "originalTable_scrambled", 0, 0);
    Dimension d1 = new Dimension(originalSchema, "originalTable_scrambled", 0, 0);
    assertEquals(
        new HyperTableCube(Arrays.asList(d1)),
        ((AggExecutionNode) queryExecutionPlan.getRootNode().getExecutableNodeBaseDependents().get(0).getExecutableNodeBaseDependents().get(0)).getAggMeta().getCubes().get(0));
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependents().get(0)).setScrambleMetaSet(meta);

//    stmt.execute("create schema if not exists `verdictdb_temp`;");
    JdbcConnection jdbcConnection = JdbcConnection.create(conn);
    jdbcConnection.setOutputDebugMessage(true);
    ExecutablePlanRunner.runTillEnd(jdbcConnection, queryExecutionPlan);
//    stmt.execute("drop schema `verdictdb_temp` cascade;");

    ExecutionInfoToken token = new ExecutionInfoToken();
    CreateTableAsSelectQuery query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createQuery(Arrays.asList(token));
    SelectQueryToSql queryToSql = new SelectQueryToSql(new MysqlSyntax());
    String actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    String expected = String.format("select " +
        "sum(vt1.`value`) as `agg0`, " +
        "count(*) as `agg1`, " +
        "min(vt1.`value`) as `agg2`, " +
        "vt1.`%s` as `verdictdb_tier_alias` " +
        "from `%s`.`originalTable_scrambled` as vt1 " +
        "where vt1.`verdictdbaggblock` = 0 group by `verdictdb_tier_alias`",
        tierColumn, originalSchema);
    assertEquals(expected, actual);

    ExecutionInfoToken token1 = new ExecutionInfoToken();
    token1.setKeyValue("schemaName", "verdict_temp");
    token1.setKeyValue("tableName", "table1");
    token1.setKeyValue("channel", 4000);
    ExecutionInfoToken token2 = new ExecutionInfoToken();
    token2.setKeyValue("schemaName", "verdict_temp");
    token2.setKeyValue("tableName", "table2");
    token2.setKeyValue("channel", 4001);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).getSources().get(1).createQuery(Arrays.asList(token1, token2));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
    expected = "select " +
        "sum(unionTable.`agg0`) as `agg0`, " +
        "sum(unionTable.`agg1`) as `agg1`, " +
        "min(unionTable.`agg2`) as `agg2`, " +
        "unionTable.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from (" +
        "select * from `verdict_temp`.`table2` as verdictdb_alias " +
        "UNION ALL " +
        "select * from `verdict_temp`.`table1` as verdictdb_alias) " +
        "as unionTable group by `verdictdb_tier_alias`";
    assertEquals(expected, actual);

    ExecutionInfoToken token3 = queryExecutionPlan.getRoot().getSources().get(0).getSources().get(0).createToken(null);
    query = (CreateTableAsSelectQuery) queryExecutionPlan.getRoot().getSources().get(0).createQuery(Arrays.asList(token3));
    actual = queryToSql.toSql(query.getSelect());
    actual = actual.replaceAll("verdictdbtemptable_\\d+_\\d+", "verdictdbtemptable");
    actual = actual.replaceAll("verdictdb_alias_\\d+_\\d+", "verdictdb_alias");
    actual = actual.replaceAll("verdictdb_tier_alias_\\d+_\\d+", "verdictdb_tier_alias");
//    System.out.println(actual);
    expected = "select " +
        "(1 + min(verdictdb_internal_tier_consolidated.`agg2`)) * " +
        "(sum(verdictdb_internal_tier_consolidated.`agg0`) / sum(verdictdb_internal_tier_consolidated.`agg1`)) as `vc2` " +
        "from (" +
        "select " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg0`) " +
        "else 0 end as `agg0`, " +
        "case " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 1) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 2) then (2.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "when (verdictdb_internal_before_scaling.`verdictdb_tier_alias` = 0) then (1.0000000000000000 * verdictdb_internal_before_scaling.`agg1`) " +
        "else 0 end as `agg1`, " +
        "verdictdb_internal_before_scaling.`agg2` as `agg2`, " +
        "verdictdb_internal_before_scaling.`verdictdb_tier_alias` as `verdictdb_tier_alias` " +
        "from `verdictdb_temp`.`verdictdbtemptable` as verdictdb_internal_before_scaling) " +
        "as verdictdb_internal_tier_consolidated";
    assertEquals(expected, actual);
  }
}
