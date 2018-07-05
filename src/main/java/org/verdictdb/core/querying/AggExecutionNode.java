package org.verdictdb.core.querying;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.ola.AggMeta;
import org.verdictdb.core.querying.ola.HyperTableCube;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;

public class AggExecutionNode extends CreateTableAsSelectNode {

  AggMeta aggMeta = new AggMeta();
  //List<HyperTableCube> cubes = new ArrayList<>();

  protected AggExecutionNode(IdCreator namer, SelectQuery query) {
    super(namer, query);
  }
  
  public static AggExecutionNode create(IdCreator namer, SelectQuery query) {
    AggExecutionNode node = new AggExecutionNode(namer, null);
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
    ExecutionInfoToken token = super.createToken(result);
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependent", this);
    return token;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AggExecutionNode node = new AggExecutionNode(namer, selectQuery);
    copyFields(this, node);
    selectQuery = selectQuery.selectListDeepCopy();
    return node;
  }

  public AggMeta getMeta() {
    return aggMeta;
  }
}
