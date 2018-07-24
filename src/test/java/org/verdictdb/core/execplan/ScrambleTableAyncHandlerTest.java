package org.verdictdb.core.execplan;
//package org.verdictdb.core.execution;
//
//import org.apache.commons.lang3.tuple.ImmutablePair;
//import org.apache.commons.lang3.tuple.Pair;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.verdictdb.core.connection.JdbcConnection;
//import org.verdictdb.core.connection.StaticMetaData;
//import org.verdictdb.core.execution.ola.AsyncAggExecutionNode;
//import org.verdictdb.core.execution.ola.AsyncQueryExecutionPlan;
//import org.verdictdb.core.execution.ola.Dimension;
//import org.verdictdb.core.execution.ola.HyperTableCube;
//import org.verdictdb.core.query.AbstractRelation;
//import org.verdictdb.core.query.CreateTableAsSelectQuery;
//import org.verdictdb.core.query.SelectQuery;
//import org.verdictdb.core.scramble.ScrambleMeta;
//import org.verdictdb.core.scramble.ScrambleMetaForTable;
//import org.verdictdb.core.scramble.UniformScrambler;
//import org.verdictdb.exception.VerdictDBException;
//import org.verdictdb.resulthandler.StandardOutputHandler;
//import org.verdictdb.resulthandler.TokenQueueToAyncHandler;
//import org.verdictdb.sql.NonValidatingSQLParser;
//import org.verdictdb.sql.QueryToSql;
//import org.verdictdb.sql.RelationStandardizer;
//import org.verdictdb.sql.syntax.H2Syntax;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
//import static java.sql.Types.BIGINT;
//import static java.sql.Types.DOUBLE;
//import static org.junit.Assert.assertEquals;
//
//public class ScrambleTableAyncHandlerTest {
//
//  static Connection conn;
//
//  static Statement stmt;
//
//  static int aggBlockCount = 3;
//
//  static ScrambleMeta meta = new ScrambleMeta();
//
//  static StaticMetaData staticMetaData = new StaticMetaData();
//
//  static String scrambledTable;
//
//  String placeholderSchemaName = "placeholderSchemaName";
//
//  String placeholderTableName = "placeholderTableName";
//
//  static String originalSchema = "originalSchema";
//
//  static String originalTable = "originalTable";
//
//  static String smallTable = "smallTable";
//
//  @BeforeClass
//  public static void setupH2Database() throws SQLException, VerdictDBException {
//    final String DB_CONNECTION = "jdbc:h2:mem:aggexecnodetest;DB_CLOSE_DELAY=-1";
//    final String DB_USER = "";
//    final String DB_PASSWORD = "";
//    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
//
//    stmt = conn.createStatement();
//    stmt.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
//    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", originalSchema, originalTable));
//    for (int i = 0; i < 10; i++) {
//      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
//          originalSchema, originalTable, i, (double) i+1));
//    }
//    stmt.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"s_id\" int, \"s_value\" double)", originalSchema, smallTable));
//    for (int i = 0; i < 10; i++) {
//      stmt.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"s_id\", \"s_value\") VALUES(%s, %f)",
//          originalSchema, smallTable, i, (double) i+1));
//    }
//
//    UniformScrambler scrambler =
//        new UniformScrambler(originalSchema, originalTable, originalSchema, "originalTable_scrambled", aggBlockCount);
//    CreateTableAsSelectQuery scramblingQuery = scrambler.createQuery();
//    stmt.executeUpdate(QueryToSql.convert(new H2Syntax(), scramblingQuery));
//    ScrambleMetaForTable tablemeta = scrambler.generateMeta();
//    scrambledTable = tablemeta.getTableName();
//    meta.insertScrambleMetaEntry(tablemeta);
//
//    staticMetaData.setDefaultSchema(originalSchema);
//    List<Pair<String, Integer>> arr = new ArrayList<>();
//    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
//        new ImmutablePair<>("value", DOUBLE)
//    ));
//    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable_scrambled"), arr);
//    arr = new ArrayList<>();
//    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
//        new ImmutablePair<>("s_value", DOUBLE)
//    ));
//    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, smallTable), arr);
//  }
//
//  @Test
//  public void ScrambleTableTest1() throws VerdictDBException,SQLException {
//    RelationStandardizer.resetItemID();
//    String sql = "select sum(value) from originalTable_scrambled";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    AbstractRelation relation = sqlToRelation.toRelation(sql);
//    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
//    relation = gen.standardize((SelectQuery) relation);
//
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
//    queryExecutionPlan.cleanUp();
//    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
//
//    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
//    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
//    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    tokenQueueToAyncHandler.execute();
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
//  }
//
//  @Test
//  public void ScrambleTableTest2() throws VerdictDBException,SQLException {
//    RelationStandardizer.resetItemID();
//    String sql = "select count(s_value) from smallTable where s_value > (select avg(value) from originalTable_scrambled)";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    AbstractRelation relation = sqlToRelation.toRelation(sql);
//    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
//    relation = gen.standardize((SelectQuery) relation);
//
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
//    queryExecutionPlan.cleanUp();
//    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
//
//    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
//    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
//    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    tokenQueueToAyncHandler.execute();
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
//  }
//
//  @Test
//  public void ScrambleTableWithScalingTest() throws VerdictDBException,SQLException {
//    RelationStandardizer.resetItemID();
//    String sql = "select sum(value) from originalTable_scrambled";
//    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
//    AbstractRelation relation = sqlToRelation.toRelation(sql);
//    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
//    relation = gen.standardize((SelectQuery) relation);
//
//    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp", meta, (SelectQuery) relation);
//    queryExecutionPlan.cleanUp();
//    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
//    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().dependents.get(0)).setScrambleMeta(meta);
//    queryExecutionPlan.setScalingNode();
//
//    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
//    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
//    stmt.execute("create schema if not exists \"verdictdb_temp\";");
//    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    tokenQueueToAyncHandler.execute();
//    stmt.execute("drop schema \"verdictdb_temp\" cascade;");
//  }
//}
