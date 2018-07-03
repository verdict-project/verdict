package org.verdictdb.core.querying;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;
import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlreader.QueryToSql;
import org.verdictdb.sqlsyntax.H2Syntax;

public class DropTableExecutionNodeTest {
  
  static String originalSchema = "originalschema";

  static String originalTable = "originalschema";

  static DbmsConnection conn;

  @BeforeClass
  public static void setupDbConnAndScrambledTable() throws SQLException, VerdictDBException {
    final String DB_CONNECTION = "jdbc:h2:mem:droptablenodetest;DB_CLOSE_DELAY=-1";
    final String DB_USER = "";
    final String DB_PASSWORD = "";
    conn = new JdbcConnection(DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD), new H2Syntax());
    conn.execute(String.format("CREATE SCHEMA \"%s\"", originalSchema));
    populateData(conn, originalSchema, originalTable);
  }

  @Test
  public void testExecuteNode() throws VerdictDBException {
//    LinkedBlockingDeque<ExecutionResult> resultQueue = new LinkedBlockingDeque<>();
    ExecutableNodeBase root = DropTableExecutionNode.create();
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("schemaName", originalSchema);
    token.setKeyValue("tableName", originalTable);
    conn.execute(QueryToSql.convert(new H2Syntax(), root.createQuery(Arrays.asList(token))));
  }

  static void populateData(DbmsConnection conn, String schemaName, String tableName) throws VerdictDBDbmsException {
    conn.execute(String.format("CREATE TABLE \"%s\".\"%s\"(\"id\" int, \"value\" double)", schemaName, tableName));
    for (int i = 0; i < 2; i++) {
      conn.execute(String.format("INSERT INTO \"%s\".\"%s\"(\"id\", \"value\") VALUES(%s, %f)",
          schemaName, tableName, i, (double) i+1));
    }
  }

}
