package org.verdictdb.core.execution;

import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.exception.VerdictDBException;

public class ProjectionExecutionNode extends CreateTableAsSelectExecutionNode {

  protected ProjectionExecutionNode(String scratchpadSchemaName) {
    super(scratchpadSchemaName);
  }
  
  public static ProjectionExecutionNode create(SelectQuery query, String scratchpadSchemaName) {
    ProjectionExecutionNode node = new ProjectionExecutionNode(scratchpadSchemaName);
    SubqueriesToDependentNodes.convertSubqueriesIntoDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }
  
  public SelectQuery getSelectQuery() {
    return (SelectQuery) selectQuery;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    return super.executeNode(conn, downstreamResults);
  }

  @Override
  public QueryExecutionNode deepcopy() {
    ProjectionExecutionNode node = new ProjectionExecutionNode(scratchpadSchemaName);
    copyFields(this, node);
    return node;
  }
}
