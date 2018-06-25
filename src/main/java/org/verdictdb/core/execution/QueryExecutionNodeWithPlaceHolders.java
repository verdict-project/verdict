package org.verdictdb.core.execution;

import java.util.ArrayList;
import java.util.List;

import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.BaseTable;
import org.verdictdb.core.query.SelectQuery;

public abstract class QueryExecutionNodeWithPlaceHolders extends QueryExecutionNode {
  
  List<BaseTable> placeholderTables = new ArrayList<>();
  
  public QueryExecutionNodeWithPlaceHolders() {
    super();
  }

  public QueryExecutionNodeWithPlaceHolders(SelectQuery query) {
    super(query);
  }
  
  BaseTable createPlaceHolderTable(String aliasName) {
    BaseTable table = new BaseTable("placeholderSchemaName", "placeholderTableName", aliasName);
    placeholderTables.add(table);
    return table;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) {
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

}
