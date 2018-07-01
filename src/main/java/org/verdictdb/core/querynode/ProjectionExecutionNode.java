package org.verdictdb.core.querynode;

import java.util.List;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class ProjectionExecutionNode extends CreateTableAsSelectExecutionNode {

  protected ProjectionExecutionNode(QueryExecutionPlan plan) {
    super(plan);
  }
  
  public static ProjectionExecutionNode create(QueryExecutionPlan plan, SelectQuery query) throws VerdictDBValueException {
    ProjectionExecutionNode node = new ProjectionExecutionNode(plan);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
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
    ProjectionExecutionNode node = new ProjectionExecutionNode(plan);
    copyFields(this, node);
    return node;
  }
}
