package org.verdictdb.core.querying.ola;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.execplan.ExecutablePlanRunner;
import org.verdictdb.core.querying.*;
import org.verdictdb.core.scrambling.ScrambleMeta;
import org.verdictdb.core.scrambling.ScrambleMetaSet;
import org.verdictdb.core.scrambling.SimpleTreePlan;
import org.verdictdb.core.scrambling.UniformScrambler;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.H2Syntax;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlwriter.QueryToSql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

public class AsyncQueryExecutionPlanTest {
  
  static Connection conn;
  
  static String originalSchema = "originalschema";

  static String originalTable = "originalschema";

  static String newSchema = "newschema";

  static String newTable = "newtable";
  
  static ScrambleMetaSet scrambleMeta = new ScrambleMetaSet();
  
  static int aggBlockCount = 3;
  
  @BeforeClass
  public static void setupH2Database() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggexecnodeblocktest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
    conn.createStatement().execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.createStatement().execute(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateRandomData(conn, originalSchema, originalTable);
    
    // create scrambled table
    UniformScrambler scrambler =
        new UniformScrambler(originalSchema, originalTable, newSchema, newTable, aggBlockCount);
    CreateTableAsSelectQuery createQuery = scrambler.createQuery();
    String scrambleSql = QueryToSql.convert(new H2Syntax(), createQuery);
    conn.createStatement().execute(scrambleSql);
    ScrambleMeta metaEntry = scrambler.generateMeta();
    metaEntry.setNumberOfTiers(1);
    HashMap<Integer, List<Double>> distribution1 = new HashMap<>();
    distribution1.put(0, Arrays.asList(0.2, 0.5, 1.0));
    metaEntry.setCumulativeDistributionForTier(distribution1);
    scrambleMeta.addScrambleMeta(metaEntry);
  }
  
  @AfterClass
  public static void closeH2Connection() throws SQLException {
    conn.close();
  }

  @Test
  public void testConvertFlatToProgressiveAgg() throws VerdictDBException {
//    System.out.println("test case starts");
    SelectQuery aggQuery = SelectQuery.create(
        new AliasedColumn(ColumnOp.count(), "agg"),
        new BaseTable(newSchema, newTable, "t"));
    QueryExecutionPlan plan = QueryExecutionPlanFactory.create(newSchema, scrambleMeta, aggQuery);

    AggExecutionNode aggnode = AggExecutionNode.create(plan, aggQuery);
    AsyncQueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
    AggExecutionNodeBlock block = new AggExecutionNodeBlock(aggnode);
    
    ExecutableNodeBase converted = asyncPlan.convertToProgressiveAgg(scrambleMeta, block);   // AsyncAggregation
//    converted.print();
    assertTrue(converted instanceof AsyncAggExecutionNode);
    
    assertTrue(converted.getExecutableNodeBaseDependent(0) instanceof AggExecutionNode);
    for (int i = 1; i < aggBlockCount; i++) {
      assertTrue(converted.getExecutableNodeBaseDependent(i) instanceof AggCombinerExecutionNode);
      if (i == 1) {
        assertTrue(converted.getExecutableNodeBaseDependent(i).getExecutableNodeBaseDependent(0) instanceof AggExecutionNode);
        assertTrue(converted.getExecutableNodeBaseDependent(i).getExecutableNodeBaseDependent(1) instanceof AggExecutionNode);
      } else {
        assertTrue(converted.getExecutableNodeBaseDependent(i).getExecutableNodeBaseDependent(0) instanceof AggCombinerExecutionNode);
        assertTrue(converted.getExecutableNodeBaseDependent(i).getExecutableNodeBaseDependent(1) instanceof AggExecutionNode);
      }
    }
    
    ((AsyncAggExecutionNode) converted).setScrambleMetaSet(scrambleMeta);
    ExecutablePlanRunner.runTillEnd(
        new JdbcConnection(conn, new H2Syntax()), 
        new SimpleTreePlan(converted));
  }
  
  @Test
  public void testConvertNestedAgg() throws VerdictDBException {
    SelectQuery projQuery = SelectQuery.create(
        new AliasedColumn(new BaseColumn("t", "value"), "v"),
        new BaseTable(newSchema, newTable, "t"));
    projQuery.setAliasName("t2");
    SelectQuery aggQuery = SelectQuery.create(
        new AliasedColumn(ColumnOp.count(), "agg"),
        projQuery);
    
    QueryExecutionPlan plan = QueryExecutionPlanFactory.create(newSchema, scrambleMeta, aggQuery);
    AsyncQueryExecutionPlan asyncPlan = AsyncQueryExecutionPlan.create(plan);
    ExecutableNodeBase converted = asyncPlan.getRoot();
    converted.print();
    
    List<ExecutableNode> nodes = asyncPlan.getAllNodes();
    for (ExecutableNode n : nodes) {
      SelectQuery q = ((QueryNodeBase) n).getSelectQuery();
      System.out.println(QueryToSql.convert(new HiveSyntax(), q));
    }
  }
  
  static void populateRandomData(Connection conn, String schemaName, String tableName) throws SQLException {
    Statement stmt = conn.createStatement();
    stmt.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    Random r = new Random();
    for (int i = 0; i < 10; i++) {
      stmt.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, r.nextDouble()));
    }
    stmt.close();
  }

}
