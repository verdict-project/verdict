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

public class AsyncAggExecutionPlanTest {

  static Connection conn;

  static Statement stmt;

  static int aggBlockCount = 3;

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
    final String DB_CONNECTION = "jdbc:h2:mem:asyncaggexecnodetest;DB_CLOSE_DELAY=-1";
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
    scrambledTable = tablemeta.getTableName();
    tablemeta.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution1 = new HashMap<>();
    distribution1.put(0, Arrays.asList(0.2, 0.5, 1.0));
    tablemeta.setCumulativeDistributionForTier(distribution1);
    meta.addScrambleMeta(tablemeta);

    staticMetaData.setDefaultSchema(originalSchema);
    List<Pair<String, Integer>> arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("id", BIGINT),
        new ImmutablePair<>("value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, "originalTable_scrambled"), arr);
    arr = new ArrayList<>();
    arr.addAll(Arrays.asList(new ImmutablePair<>("s_id", BIGINT),
        new ImmutablePair<>("s_value", DOUBLE)
    ));
    staticMetaData.addTableData(new StaticMetaData.TableInfo(originalSchema, smallTable), arr);
  }

  @Test
  public void ScrambleTableTest1() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = 
        QueryExecutionPlanFactory.create("verdictdb_temp1", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
//    queryExecutionPlan.getRoot().print();

    assertEquals(3, queryExecutionPlan.getRoot().getDependentNodeCount());
    stmt.execute("create schema if not exists \"verdictdb_temp1\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp1\" cascade;");
  }

  @Test
  public void ScrambleTableTest2() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(s_value) from smallTable where s_value > (select avg(value) from originalTable_scrambled)";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp2", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);

    stmt.execute("create schema if not exists \"verdictdb_temp2\";");
    ExecutablePlanRunner.runTillEnd(new JdbcConnection(conn, new H2Syntax()), queryExecutionPlan);
    stmt.execute("drop schema \"verdictdb_temp2\" cascade;");
  }

  @Test
  public void ScrambleTableWithScalingTest() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select sum(value) from originalTable_scrambled";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp3", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
    // queryExecutionPlan.getRoot().print();
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot()).setScrambleMetaSet(meta);

//    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
//    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    stmt.execute("create schema if not exists \"verdictdb_temp3\";");
    JdbcConnection jdbcconn = new JdbcConnection(conn, new H2Syntax());
    ExecutablePlanRunner.runTillEnd(jdbcconn, queryExecutionPlan);
//    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    tokenQueueToAyncHandler.execute();
//    stmt.execute("drop schema \"verdictdb_temp3\" cascade;");
    jdbcconn.execute("drop schema \"verdictdb_temp3\" cascade;");
  }

  @Test
  public void ScrambleTableWithScalingTest2() throws VerdictDBException,SQLException {
    RelationStandardizer.resetItemID();
    String sql = "select count(s_value) from smallTable where s_value > (select avg(value) from originalTable_scrambled)";
    NonValidatingSQLParser sqlToRelation = new NonValidatingSQLParser();
    AbstractRelation relation = sqlToRelation.toRelation(sql);
    RelationStandardizer gen = new RelationStandardizer(staticMetaData);
    relation = gen.standardize((SelectQuery) relation);

    QueryExecutionPlan queryExecutionPlan = QueryExecutionPlanFactory.create("verdictdb_temp4", meta, (SelectQuery) relation);
    queryExecutionPlan.cleanUp();
    queryExecutionPlan = AsyncQueryExecutionPlan.create(queryExecutionPlan);
//<<<<<<< HEAD:src/test/java/org/verdictdb/core/querying/ola/AsyncAggExecutionPlanTest.java
    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getExecutableNodeBaseDependent(0).getExecutableNodeBaseDependent(0)).setScrambleMetaSet(meta);
//    queryExecutionPlan.setScalingNode();
    // queryExecutionPlan.getRoot().print();
//=======
//    ((AsyncAggExecutionNode)queryExecutionPlan.getRoot().getDependent(0).getDependent(0)).setScrambleMetaSet(meta);
//>>>>>>> origin/joezhong-scale:src/test/java/org/verdictdb/core/execution/AsyncAggAsyncHandlerTest.java

//    TokenQueueToAyncHandler tokenQueueToAyncHandler = new TokenQueueToAyncHandler(queryExecutionPlan, new ExecutionTokenQueue());
//    tokenQueueToAyncHandler.setHandler(new StandardOutputHandler());
    stmt.execute("create schema if not exists \"verdictdb_temp4\";");
    JdbcConnection jdbcconn = new JdbcConnection(conn, new H2Syntax());
    ExecutablePlanRunner.runTillEnd(jdbcconn, queryExecutionPlan);
//    queryExecutionPlan.root.executeAndWaitForTermination(new JdbcConnection(conn, new H2Syntax()));
//    tokenQueueToAyncHandler.execute();
//    stmt.execute("drop schema \"verdictdb_temp4\" cascade;");
    jdbcconn.execute("drop schema \"verdictdb_temp4\" cascade;");
  }
}
