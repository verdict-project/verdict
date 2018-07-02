package org.verdictdb.core.querying.ola;

import static org.junit.Assert.assertEquals;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenReader;
import org.verdictdb.core.querying.AggExecutionNode;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.SimpleTreePlan;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.ConstantColumn;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlreader.QueryToSql;
import org.verdictdb.sqlsyntax.H2Syntax;

public class AggCombinerExecutionNodeTest {
  
  static DbmsConnection conn;
  
  static String originalSchema = "originalschema";

  static String originalTable = "originaltable";
  
  static String newSchema = "newschema";

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:aggcombinertest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.execute(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  @Test
  public void testSingleAggCombining() throws VerdictDBValueException {
    QueryExecutionPlan plan = new QueryExecutionPlan("newschema");
    
    BaseTable base = new BaseTable("myschema", "mytable", "t");
    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "acount"), base);
    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "acount"), base);
    
    AggExecutionNode leftNode = AggExecutionNode.create(plan, leftQuery);
    AggExecutionNode rightNode = AggExecutionNode.create(plan, rightQuery);
    
    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(plan, leftNode, rightNode);
    combiner.print();
    
    assertEquals(combiner.getListeningQueue(0), leftNode.getBroadcastingQueue(0));
    assertEquals(combiner.getListeningQueue(1), rightNode.getBroadcastingQueue(0));
  }
  
  // Test if the combined answer is identical to the original answer
  @Test
  public void testSingleAggCombiningWithH2() throws VerdictDBDbmsException, VerdictDBException {
    QueryExecutionPlan plan = new QueryExecutionPlan("newschema");
    
    BaseTable base = new BaseTable(originalSchema, originalTable, "t");
    SelectQuery leftQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
    leftQuery.addFilterByAnd(ColumnOp.lessequal(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
    SelectQuery rightQuery = SelectQuery.create(new AliasedColumn(ColumnOp.count(), "mycount"), base);
    rightQuery.addFilterByAnd(ColumnOp.greater(new BaseColumn("t", "value"), ConstantColumn.valueOf(5.0)));
    
    AggExecutionNode leftNode = AggExecutionNode.create(plan, leftQuery);
    AggExecutionNode rightNode = AggExecutionNode.create(plan, rightQuery);
    
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(plan, leftNode, rightNode);
    combiner.print();
    ExecutionTokenReader reader = ExecutablePlanRunner.getTokenReader(
        conn, 
        new SimpleTreePlan(combiner));
    
//    combiner.addBroadcastingQueue(queue);
//    combiner.executeAndWaitForTermination(conn);
    
    ExecutionInfoToken token = reader.next();
    String schemaName = (String) token.getValue("schemaName");
    String tableName = (String) token.getValue("tableName");
    
    DbmsQueryResult result = conn.execute(QueryToSql.convert(
        new H2Syntax(),
        SelectQuery.create(ColumnOp.count(), base)));
    result.next();
    int expectedCount = Integer.valueOf(result.getValue(0).toString());
    
    DbmsQueryResult result2 = conn.execute(QueryToSql.convert(
        new H2Syntax(),
        SelectQuery.create(new AsteriskColumn(), new BaseTable(schemaName, tableName, "t"))));
    result2.next();
    int actualCount = Integer.valueOf(result2.getValue(0).toString());
    assertEquals(expectedCount, actualCount);
    conn.execute(QueryToSql.convert(
        new H2Syntax(),
        DropTableQuery.create(schemaName, tableName)));
  }
  
  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 10; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

}
