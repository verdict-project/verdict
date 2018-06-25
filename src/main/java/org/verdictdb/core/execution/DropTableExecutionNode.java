package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.sql.DropTableToSql;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDbException;

public class DropTableExecutionNode extends QueryExecutionNode {
  
  public DropTableExecutionNode() {
    super(null);
  }
  
  public static DropTableExecutionNode create() {
    DropTableExecutionNode node = new DropTableExecutionNode();
    return node;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    ExecutionResult result = downstreamResults.get(0);
    String schemaName = (String) result.getValue("schemaName");
    String tableName = (String) result.getValue("tableName");
    
    DropTableQuery dropQuery = new DropTableQuery(schemaName, tableName);
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), dropQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    return ExecutionResult.empty();
  }

}
