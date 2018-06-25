package org.verdictdb.core.execution;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDBException;

public class CreateTableAsSelectExecutionNode extends QueryExecutionNodeWithPlaceHolders {
  
  CreateTableAsSelectQuery createQuery;
  
  String newTableSchemaName;
  
  String newTableName;
  
  String scratchpadSchemaName;
  
  static int tempTableNameNum = 0;
  
  protected CreateTableAsSelectExecutionNode(String scratchpadSchemaName) {
    super();
    this.scratchpadSchemaName = scratchpadSchemaName;
    
    Pair<String, String> tempTableFullName = generateTempTableName();
    this.newTableSchemaName = tempTableFullName.getLeft();
    this.newTableName = tempTableFullName.getRight();
  }
  
//  public void setNewTableSchemaName(String schemaName) {
//    this.newTableSchemaName = schemaName;
//  }
//  
//  public void setNewTableName(String tableName) {
//    this.newTableName = tableName;
//  }
  
  public static CreateTableAsSelectExecutionNode create(SelectQuery query, String scratchpadSchemaName) {
    CreateTableAsSelectExecutionNode node = new CreateTableAsSelectExecutionNode(scratchpadSchemaName);
    node.setSelectQuery(query);
    return node;
  }
  
  public void setSelectQuery(SelectQuery query) {
    this.selectQuery = query;
    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(newTableSchemaName, newTableName, query);
    this.createQuery = createQuery;
  }
  
  public SelectQuery getSelectQuery() {
    return (SelectQuery) selectQuery;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) {
    super.executeNode(conn, downstreamResults);
    
//    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(schemaName, tableName, query);
//    CreateTableToSql toSql = new CreateTableToSql(conn.getSyntax());
    try {
      String sql = QueryToSql.convert(conn.getSyntax(), createQuery);
      conn.executeUpdate(sql);
    } catch (VerdictDBException e) {
      e.printStackTrace();
    }
    
    // write the result
    ExecutionInfoToken result = new ExecutionInfoToken();
    result.setKeyValue("schemaName", newTableSchemaName);
    result.setKeyValue("tableName", newTableName);
    return result;
  }
  
  Pair<String, String> generateTempTableName() {
    return Pair.of(scratchpadSchemaName, String.format("verdictdbtemptable_%d", tempTableNameNum++));
  }

  @Override
  public QueryExecutionNode deepcopy() {
    CreateTableAsSelectExecutionNode node = new CreateTableAsSelectExecutionNode(scratchpadSchemaName);
    copyFields(this, node);
    return node;
  }
  
  void copyFields(CreateTableAsSelectExecutionNode from, CreateTableAsSelectExecutionNode to) {
    super.copyFields(from, to);
    to.newTableSchemaName = from.newTableSchemaName;
    to.newTableName = from.newTableName;
    to.scratchpadSchemaName = from.scratchpadSchemaName;
    to.createQuery = new CreateTableAsSelectQuery(newTableSchemaName, newTableName, to.getSelectQuery());
  }

}
