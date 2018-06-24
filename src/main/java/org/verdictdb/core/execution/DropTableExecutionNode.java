package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.sql.DropTableToSql;
import org.verdictdb.exception.VerdictDbException;

public class DropTableExecutionNode extends QueryExecutionNode {
  
  String schemaName;
  
  String tableName;
  
  public DropTableExecutionNode(
      DbmsConnection conn, 
      String schemaName, 
      String tableName) {
    super(conn, null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    DropTableQuery dropQuery = new DropTableQuery(schemaName, tableName);
    DropTableToSql toSql = new DropTableToSql(conn.getSyntax());
    try {
      String sql = toSql.toSql(dropQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    return ExecutionResult.empty();
  }

}
