package org.verdictdb.core.querying;

import static org.junit.Assert.assertEquals;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutableNodeRunner;
import org.verdictdb.core.execution.ExecutablePlanRunner;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.execution.ExecutionTokenQueue;
import org.verdictdb.core.querying.QueryExecutionPlan;
import org.verdictdb.core.querying.SelectAllExecutionNode;
import org.verdictdb.core.querying.SimpleTreePlan;
import org.verdictdb.core.querying.TempIdCreatorInScratchPadSchema;
import org.verdictdb.core.sqlobject.AliasedColumn;
import org.verdictdb.core.sqlobject.BaseColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.ColumnOp;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SubqueryColumn;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.resulthandler.StandardOutputPrinter;
import org.verdictdb.sqlsyntax.H2Syntax;

public class SelectAllExecutionNodeTest {

  static String originalSchema = "originalschema";

  static String originalTable = "originaltable";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static DbmsConnection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:createasselecttest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", originalSchema));
    conn.execute(String.format("CREATE SCHEMA IF NOT EXISTS\"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 2; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

  @Test
  public void testGenerateDependency()  throws VerdictDBException {
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t1", "value")), "a")),
        new BaseTable(originalSchema, originalTable, "t1"));
    SelectQuery query = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("t", "value"), "average")),
        new BaseTable(originalSchema, originalTable, "t"));
    query.addFilterByAnd(new ColumnOp("greater", Arrays.asList(
        new BaseColumn("t", "value"),
        new SubqueryColumn(subquery)
    )));
    QueryExecutionPlan plan = new QueryExecutionPlan(newSchema);
    SelectAllExecutionNode node = SelectAllExecutionNode.create(plan, query);
    String aliasName = String.format("verdictdbalias_%d_0", plan.getSerialNumber());

    assertEquals(1, node.dependents.size());
    assertEquals(1, node.dependents.get(0).dependents.size());
    SelectQuery rewritten = SelectQuery.create(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn("placeholderSchemaName", aliasName, "a"), "a"))
        , new BaseTable("placeholderSchemaName", "placeholderTableName", aliasName));
    assertEquals(
        rewritten, 
        ((SubqueryColumn)((ColumnOp) ((SelectQuery) node.dependents.get(0).getSelectQuery()).getFilter().get()).getOperand(1)).getSubquery());
  }

  //
  // select 
  @Test
  public void testExecuteNode() throws VerdictDBException {
    SelectQuery subquery = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t1", "value")), "a")),
        new BaseTable(originalSchema, originalTable, "t1"));
    SelectQuery query = SelectQuery.create(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("t", "value"), "average")),
        new BaseTable(originalSchema, originalTable, "t"));
    query.addFilterByAnd(new ColumnOp("greater", Arrays.asList(
        new BaseColumn("t", "value"),
        new SubqueryColumn(subquery)
    )));

//    QueryExecutionPlan plan = new QueryExecutionPlan(newSchema);
    SelectAllExecutionNode node = SelectAllExecutionNode.create(
        new TempIdCreatorInScratchPadSchema(newSchema), query);
//    conn.executeUpdate(String.format("create table \"%s\".\"%s\"", newSchema, ((ProjectionExecutionNode)node.dependents.get(0)).newTableName));
//    ExecutionInfoToken subqueryToken = new ExecutionInfoToken();
//    subqueryToken.setKeyValue("schemaName", ((ProjectionExecutionNode)node.dependents.get(0)).newTableSchemaName);
//    subqueryToken.setKeyValue("tableName", ((ProjectionExecutionNode)node.dependents.get(0)).newTableName);
//    node.executeNode(conn, Arrays.asList(subqueryToken));
//    conn.executeUpdate(String.format("drop table \"%s\".\"%s\"", newSchema, ((ProjectionExecutionNode)node.dependents.get(0)).newTableName));
//    ExecutionTokenQueue queue = new ExecutionTokenQueue();
//    node.addBroadcastingQueue(queue);
//    node.executeAndWaitForTermination(conn);
//    StandardOutputPrinter.run(ExecutablePlanRunner.run(conn, plan));
//    ExecutableNodeRunner.run(conn, node);
//    ExecutableNodeRunner.execute(conn, node);
    ExecutablePlanRunner.runTillEnd(conn, new SimpleTreePlan(node));
  }

  @AfterClass
  static public void clean() throws VerdictDBDbmsException {
    conn.execute(String.format("Drop TABLE \"%s\".\"%s\"", originalSchema, originalTable));
  }
}
