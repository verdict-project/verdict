package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.exception.VerdictDBException;

public class AggExecutionNode extends CreateTableAsSelectExecutionNode {

  protected AggExecutionNode(String scratchpadSchemaName) {
    super(scratchpadSchemaName);
  }
  
  public static AggExecutionNode create(SelectQuery query, String scratchpadSchemaName) {
    AggExecutionNode node = new AggExecutionNode(scratchpadSchemaName);
    SubqueriesToDependentNodes.convertSubqueriesIntoDependentNodes(query, node);
    node.setSelectQuery(query);
    
    return node;
  }
  
  public SelectQuery getSelectQuery() {
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    return super.executeNode(conn, downstreamResults);
  }

  @Override
  public QueryExecutionNode deepcopy() {
    AggExecutionNode node = new AggExecutionNode(scratchpadSchemaName);
    copyFields(this, node);
    return node;
  }
  
}
