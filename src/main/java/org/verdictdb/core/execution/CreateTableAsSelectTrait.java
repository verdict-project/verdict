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
  
  protected CreateTableAsSelectTrait() {
    super();
  }
  
  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }
  
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }
  
  public static CreateTableAsSelectTrait create(QueryExecutionPlan plan, SelectQuery query) {
    CreateTableAsSelectTrait node = new CreateTableAsSelectTrait();
    
    Pair<String, String> tempTableFullName = plan.generateTempTableName();
    node.setSchemaName(tempTableFullName.getLeft());
    node.setTableName(tempTableFullName.getRight());
    
    return node;
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

}
