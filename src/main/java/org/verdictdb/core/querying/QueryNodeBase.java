package org.verdictdb.core.querying;

import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execplan.ExecutableNode;
import org.verdictdb.core.execplan.ExecutionInfoToken;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class QueryNodeBase extends ExecutableNodeBase {
  
  private static final long serialVersionUID = 7263437396821994391L;
  
  protected SelectQuery selectQuery;
  
  public QueryNodeBase(SelectQuery selectQuery) {
    this.selectQuery = selectQuery;
  }
  
  List<ExecutableNode> getParents() {
    return getSubscribers();
  }
  
  public SelectQuery getSelectQuery() {
    return selectQuery;
  }
  
  public void setSelectQuery(SelectQuery query) {
    this.selectQuery = query;
  }
  
  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    return selectQuery;
  }

  @Override
  public ExecutionInfoToken createToken(DbmsQueryResult result) {
    ExecutionInfoToken token = new ExecutionInfoToken();
    token.setKeyValue("queryResult", result);
    return token;
  }
  
  @Override
  public ExecutableNodeBase deepcopy() {
    QueryNodeBase node = new QueryNodeBase(selectQuery);
    copyFields(this, node);
    return node;
  }
  
  protected void copyFields(QueryNodeBase from, QueryNodeBase to) {
    super.copyFields(from, to);
    to.selectQuery = from.selectQuery.deepcopy();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    QueryNodeBase rhs = (QueryNodeBase) obj;
    return new EqualsBuilder()
                  .appendSuper(super.equals(obj))
                  .append(selectQuery, rhs.selectQuery)
                  .isEquals();
  }

}
