package org.verdictdb.core.execution;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.*;
import org.verdictdb.core.sql.CreateTableToSql;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDbException;

public class CreateTableAsSelectTrait extends QueryExecutionNodeWithPlaceHolders {
  
  String schemaName;
  
  String tableName;
  
  String scratchpadSchemaName;
  
  static int tempTableNameNum = 0;
  
  protected CreateTableAsSelectTrait(String scratchpadSchemaName) {
    super();
    this.scratchpadSchemaName = scratchpadSchemaName;
  }
  
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public static CreateTableAsSelectTrait create(SelectQuery query, String scratchpadSchemaName) {
    CreateTableAsSelectTrait node = new CreateTableAsSelectTrait(scratchpadSchemaName);
    
    Pair<String, String> tempTableFullName = node.generateTempTableName();
    node.setSchemaName(tempTableFullName.getLeft());
    node.setTableName(tempTableFullName.getRight());
    
    return node;
  }
  
  public SelectQuery getQuery() {
    return (SelectQuery) query;
  }

  @Override
  public ExecutionResult executeNode(List<ExecutionResult> downstreamResults) {
    super.executeNode(downstreamResults);
    
//    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(schemaName, tableName, query);
//    CreateTableToSql toSql = new CreateTableToSql(conn.getSyntax());
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), query);
      conn.executeUpdate(sql);
    } catch (VerdictDbException e) {
      e.printStackTrace();
    }
    
    // write the result
    ExecutionResult result = new ExecutionResult();
    result.setKeyValue("schemaName", schemaName);
    result.setKeyValue("tableName", tableName);
    return result;
  }
  
  Pair<String, String> generateTempTableName() {
    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%d", tempTableNameNum++));
  }

}
