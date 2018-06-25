package org.verdictdb.core.execution;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.connection.JdbcConnection;
import org.verdictdb.core.query.*;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.H2Syntax;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ProjectionExecutionNodeTest {

  static String originalSchema = "originalschema";

  static String originalTable = "originaltable";

  static String newSchema = "newschema";

  static String newTable  = "newtable";

  static int aggblockCount = 2;

  static DbmsConnection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDbException {
    final String DB_CONNECTION = "jdbc:h2:mem:createasselecttest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.executeUpdate(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    conn.executeUpdate(String.format("CREATE SCHEMA \"%s\"", newSchema));
    populateData(conn, originalSchema, originalTable);
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws SQLException {
    conn.executeUpdate(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 2; i++) {
      conn.executeUpdate(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

  @Test
  public void testGenerateDependency()  throws VerdictDbException {
    SelectQuery subquery = SelectQuery.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t1", "value")), "a")),
        new BaseTable(originalSchema, originalTable, "t1"));
    SelectQuery query = SelectQuery.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("t", "value"), "average")),
        new BaseTable(originalSchema, originalTable, "t"));
    query.addFilterByAnd(new ColumnOp("greater", Arrays.asList(
        new BaseColumn("t", "value"),
        new SubqueryColumn(subquery)
    )));
    ProjectionExecutionNode node = new ProjectionExecutionNode(conn, newSchema, newTable, query);
    QueryExecutionPlan.resetTempTableNameNum();

    assertEquals(1, node.dependents.size());
    SelectQuery rewritten = SelectQuery.getSelectQueryOp(
        Arrays.<SelectItem>asList(
            new AliasedColumn(new BaseColumn(newSchema,"verdictdbtemptable_0", "a"), "a"))
        , new BaseTable("newschema", "verdictdbtemptable_0", "verdictdbtemptable_0"));
    assertEquals(rewritten, ((SubqueryColumn)((ColumnOp)node.query.getFilter().get()).getOperand(1)).getSubquery());
  }

  @Test
  public void testExecuteNode() throws VerdictDbException {
    SelectQuery subquery = SelectQuery.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new ColumnOp("avg", new BaseColumn("t1", "value")), "a")),
        new BaseTable(originalSchema, originalTable, "t1"));
    SelectQuery query = SelectQuery.getSelectQueryOp(
        Arrays.<SelectItem>asList(new AliasedColumn(new BaseColumn("t", "value"), "average")),
        new BaseTable(originalSchema, originalTable, "t"));
    query.addFilterByAnd(new ColumnOp("greater", Arrays.asList(
        new BaseColumn("t", "value"),
        new SubqueryColumn(subquery)
    )));
    ProjectionExecutionNode node = new ProjectionExecutionNode(conn, newSchema, newTable, query);
    QueryExecutionPlan.resetTempTableNameNum();

    node.execute();
    conn.executeUpdate(String.format("DROP TABLE \"%s\".\"%s\"", newSchema, newTable));
    conn.executeUpdate(String.format("DROP TABLE \"%s\".\"%s\"", newSchema, "verdictdbtemptable_0"));
  }
}
