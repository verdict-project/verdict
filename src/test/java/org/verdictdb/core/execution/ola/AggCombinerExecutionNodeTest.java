package org.verdictdb.core.execution.ola;

import static org.junit.Assert.assertEquals;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.execution.AggExecutionNode;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.execution.QueryExecutionPlan;
import org.verdictdb.core.query.AliasedColumn;
import org.verdictdb.core.query.AsteriskColumn;
import org.verdictdb.core.query.BaseColumn;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.ColumnOp;
import org.verdictdb.core.query.ConstantColumn;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sql.syntax.H2Syntax;

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
    conn.executeUpdate(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.executeUpdate(String.format("CREATE SCHEMA \"%s\"", newSchema));
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
    
    ExecutionTokenQueue queue = new ExecutionTokenQueue();
    AggCombinerExecutionNode combiner = AggCombinerExecutionNode.create(plan, leftNode, rightNode);
    combiner.print();
    combiner.addBroadcastingQueue(queue);
    combiner.executeAndWaitForTermination(conn);
    
    ExecutionInfoToken token = queue.take();
    String schemaName = (String) token.getValue("schemaName");
    String tableName = (String) token.getValue("tableName");
    
    DbmsQueryResult result = conn.executeQuery(QueryToSql.convert(
        new H2Syntax(),
        SelectQuery.create(ColumnOp.count(), base)));
    result.next();
    int expectedCount = Integer.valueOf(result.getValue(0).toString());
    
    DbmsQueryResult result2 = conn.executeQuery(QueryToSql.convert(
        new H2Syntax(),
        SelectQuery.create(new AsteriskColumn(), new BaseTable(schemaName, tableName, "t"))));
    result2.next();
    int actualCount = Integer.valueOf(result2.getValue(0).toString());
    assertEquals(expectedCount, actualCount);
    conn.executeUpdate(QueryToSql.convert(
        new H2Syntax(),
        DropTableQuery.create(schemaName, tableName)));
  }
  
  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 10; i++) {
      conn.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

}
