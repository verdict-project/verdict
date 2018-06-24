package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.exception.VerdictDbException;

public class ProjectionExecutionNode extends CreateTableAsSelectExecutionNode {


  public ProjectionExecutionNode(DbmsConnection conn, String schemaName, String tableName, SelectQuery query) {
    super(conn, schemaName, tableName, query);
    try {
      generateDependency();
    } catch (VerdictDbException e){
      e.printStackTrace();
    }
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    return null;
  }
}
