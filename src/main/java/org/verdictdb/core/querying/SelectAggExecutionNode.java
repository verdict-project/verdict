package org.verdictdb.core.querying;

import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.querying.ola.AggMeta;
import org.verdictdb.core.querying.ola.AsyncAggExecutionNode;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

import java.util.List;

public class SelectAggExecutionNode extends QueryNodeBase {

  private static final long serialVersionUID = 47947858649322912L;

  public SelectAggExecutionNode(IdCreator idCreator, SelectQuery selectQuery) {
    super(idCreator, selectQuery);
  }

  public static SelectAggExecutionNode create(AggExecutionNode node) {
    SelectAggExecutionNode selectAggExecutionNode = new SelectAggExecutionNode(node.namer, node.selectQuery);
    selectAggExecutionNode.aggMeta = node.aggMeta;
    return selectAggExecutionNode;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("queryResult", result);
    token.setKeyValue("aggMeta", aggMeta);
    token.setKeyValue("dependentQuery", this.selectQuery);
    return token;
  }

}
