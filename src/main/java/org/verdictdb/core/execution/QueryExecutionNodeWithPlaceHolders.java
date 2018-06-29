package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public abstract class QueryExecutionNodeWithPlaceHolders extends QueryExecutionNode {
  
  List<BaseTable> placeholderTables = new ArrayList<>();
  
  public QueryExecutionNodeWithPlaceHolders(QueryExecutionPlan plan) {
    super(plan);
  }

  public QueryExecutionNodeWithPlaceHolders(QueryExecutionPlan plan, SelectQuery query) {
    super(plan, query);
  }
  
  public Pair<BaseTable, ExecutionTokenQueue> createPlaceHolderTable(String aliasName) throws VerdictDBValueException {
    BaseTable table = new BaseTable("placeholderSchemaName", "placeholderTableName", aliasName);
    placeholderTables.add(table);
    ExecutionTokenQueue listeningQueue = generateListeningQueue();
    return Pair.of(table, listeningQueue);
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    if (downstreamResults.size() < placeholderTables.size()) {
      throw new VerdictDBValueException("Not enough temp tables to plug into placeholder tables.");
    }
    
    for (int i = 0; i < placeholderTables.size(); i++) {
      BaseTable t = placeholderTables.get(i);
      ExecutionInfoToken r = downstreamResults.get(i);
      String schemaName = (String) r.getValue("schemaName");
      String tableName = (String) r.getValue("tableName");
      t.setSchemaName(schemaName);
      t.setTableName(tableName);
    }
    return null;
  }

  public List<BaseTable> getPlaceholderTables() {
    return placeholderTables;
  }
}
