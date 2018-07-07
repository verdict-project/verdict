package org.verdictdb.core.querying;

import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class ProjectionNode extends CreateTableAsSelectNode {
  
  public ProjectionNode(IdCreator namer, SelectQuery query) {
    super(namer, query);
  }

  public static ProjectionNode create(IdCreator namer, SelectQuery query) {
    ProjectionNode node = new ProjectionNode(namer, null);
    SubqueriesToDependentNodes.convertSubqueriesToDependentNodes(query, node);
    node.setSelectQuery(query);
    return node;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return super.createQuery(tokens);
  }
  
  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    return super.createToken(result);
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    ProjectionNode node = new ProjectionNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }
}
