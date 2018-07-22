package org.verdictdb.core.querying;

import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ola.AggMeta;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class AggExecutionNode extends CreateTableAsSelectNode {

  private static final long serialVersionUID = 6222493718874657695L;
  
  private final AggMeta aggMeta = new AggMeta();
  
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
    token.setKeyValue("dependentQuery", this.selectQuery);
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
  
  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.DEFAULT_STYLE)
        .appendSuper(super.toString())
        .append("aggmeta", aggMeta)
        .build();
  }
}
