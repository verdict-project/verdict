package org.verdictdb.core.execution;

import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.sql.DropTableToSql;
import org.verdictdb.exception.VerdictDbException;

public class DropTableExecutionNode extends QueryExecutionNode {
  
  String schemaName;
  
  String tableName;
  
  public DropTableExecutionNode(DbmsConnection conn, String schemaName, String tableName) {
    super(conn);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }

  @Override
  public void executeNode(
      List<ExecutionResult> resultFromChildren, 
      BlockingDeque<ExecutionResult> resultQueue) {
    DropTableQuery dropQuery = new DropTableQuery(schemaName, tableName);
    DropTableToSql toSql = new DropTableToSql(conn.getSyntax());
    try {
      String sql = toSql.toSql(dropQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    resultQueue.add(ExecutionResult.completeResult());
  }

}
