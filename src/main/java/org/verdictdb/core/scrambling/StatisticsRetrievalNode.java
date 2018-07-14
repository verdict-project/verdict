package org.verdictdb.core.scrambling;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsQueryResult;
import org.verdictdb.core.execution.ExecutionInfoToken;
import org.verdictdb.core.querying.ExecutableNodeBase;
import org.verdictdb.core.querying.QueryNodeBase;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;

public class StatisticsRetrievalNode extends QueryNodeBase {
  
  String schemaName;
  
  String tableName;
  
  StatiticsQueryGenerator queryGenerator;

  public StatisticsRetrievalNode(String schemaName, String tableName) {
    super(null);
    this.schemaName = schemaName;
    this.tableName = tableName;
  }
  
  public static StatisticsRetrievalNode create(
      StatiticsQueryGenerator queryGenerator, 
      String schemaName, 
      String tableName) {
    StatisticsRetrievalNode node = new StatisticsRetrievalNode(schemaName, tableName);
    node.queryGenerator = queryGenerator;
    return node;
  }

  @Override
  public SqlConvertible createQuery(List<ExecutionInfoToken> tokens) throws VerdictDBException {
    @SuppressWarnings("unchecked")
    List<Pair<String, String>> columnNamesAndTypes = 
        (List<Pair<String, String>>) tokens.get(0).getValue("columnMeta");
    selectQuery = queryGenerator.create(schemaName, tableName, columnNamesAndTypes, null);
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
    QueryNodeBase node = new StatisticsRetrievalNode(schemaName, tableName);
    copyFields(this, node);
    return node;
  }
  
  protected void copyFields(StatisticsRetrievalNode from, StatisticsRetrievalNode to) {
    super.copyFields(from, to);
    to.queryGenerator = from.queryGenerator;
  }
}
