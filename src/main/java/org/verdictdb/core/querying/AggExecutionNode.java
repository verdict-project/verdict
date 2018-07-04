package org.verdictdb.core.querying;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.ola.HyperTableCube;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class AggExecutionNode extends CreateTableAsSelectNode {

  List<HyperTableCube> cubes = new ArrayList<>();

  protected AggExecutionNode(TempIdCreator namer, SelectQuery query) {
    super(namer, query);
  }
  
  public static AggExecutionNode create(TempIdCreator namer, SelectQuery query) throws VerdictDBValueException {
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
    if (!cubes.isEmpty()) {
      token.setKeyValue("hyperTableCube", cubes);
      token.setKeyValue("dependent", this);
    }
    return token;
  }

  @Override
  public ExecutableNodeBase deepcopy() {
    AggExecutionNode node = new AggExecutionNode(namer, selectQuery);
    copyFields(this, node);
    return node;
  }

  public List<HyperTableCube> getCubes() {
    return cubes;
  }
}
