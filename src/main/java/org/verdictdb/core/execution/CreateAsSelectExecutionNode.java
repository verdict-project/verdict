package org.verdictdb.core.execution;

import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQueryOp;

public class CreateAsSelectExecutionNode extends QueryExecutionNode {
  
  String schemaName;
  
  String tableName;
  
  SelectQueryOp query;
  
  public CreateAsSelectExecutionNode(DbmsConnection conn, String schemaName, String tableName, SelectQueryOp query) {
    super(conn);
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.query = query;
  }

  @Override
  public ExecutionResult executeInternally(List<ExecutionResult> resultFromChildren) {
    // TODO Auto-generated method stub
    return null;
  }

}
