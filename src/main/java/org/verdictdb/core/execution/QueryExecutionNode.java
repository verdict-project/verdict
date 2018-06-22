package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;

import org.verdictdb.connection.DbmsConnection;

public abstract class QueryExecutionNode {
  
  DbmsConnection conn;
  
  List<QueryExecutionNode> children = new ArrayList<>();
  
  public QueryExecutionNode(DbmsConnection conn) {
    this.conn = conn;
  }

  public List<QueryExecutionNode> getChildren() {
    return children;
  }

  public void setChildren(List<QueryExecutionNode> children) {
    this.children = children;
  }
  
  public abstract void execute(BlockingDeque<ExecutionResult> resultQueue);

}
