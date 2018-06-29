package org.verdictdb.core.execution;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.connection.DbmsConnection;
import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.sql.QueryToSql;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBValueException;

public class CreateTableAsSelectExecutionNode extends QueryExecutionNodeWithPlaceHolders {
  
//  CreateTableAsSelectQuery createQuery;
  
//  String newTableSchemaName;
//  
//  String newTableName;
  
//  String scratchpadSchemaName;
  
//  String newTableSchemaName;
  
//  String newTableName;
  
  protected CreateTableAsSelectExecutionNode(QueryExecutionPlan plan) {
    super(plan);
    
  }
  
//  public void setNewTableSchemaName(String schemaName) {
//    this.newTableSchemaName = schemaName;
//  }
//  
//  public void setNewTableName(String tableName) {
//    this.newTableName = tableName;
//  }
  
  public static CreateTableAsSelectExecutionNode create(QueryExecutionPlan plan, SelectQuery query) throws VerdictDBValueException {
    CreateTableAsSelectExecutionNode node = new CreateTableAsSelectExecutionNode(plan);
    node.setSelectQuery(query);
    return node;
  }
  
  public void setSelectQuery(SelectQuery query) {
    this.selectQuery = query;
  }
  
  public SelectQuery getSelectQuery() {
    return (SelectQuery) selectQuery;
  }

  @Override
  public ExecutionInfoToken executeNode(DbmsConnection conn, List<ExecutionInfoToken> downstreamResults) 
      throws VerdictDBException {
    super.executeNode(conn, downstreamResults);

    Pair<String, String> tempTableFullName = plan.generateTempTableName();
    String newTableSchemaName = tempTableFullName.getLeft();
    String newTableName = tempTableFullName.getRight();
    CreateTableAsSelectQuery createQuery = new CreateTableAsSelectQuery(newTableSchemaName, newTableName, selectQuery);
    
    String sql = QueryToSql.convert(conn.getSyntax(), createQuery);
    conn.executeUpdate(sql);
    
    // write the result
    ExecutionInfoToken result = new ExecutionInfoToken();
    result.setKeyValue("schemaName", newTableSchemaName);
    result.setKeyValue("tableName", newTableName);
    return result;
  }
  
//  protected String generateUniqueName() {
//    return String.format("verdictdbtemptable_%d", tempTableNameNum++);
//  }
//  
//  protected Pair<String, String> generateTempTableName() {
//    return Pair.of(scratchpadSchemaName, generateUniqueName());
//  }

  @Override
  public QueryExecutionNode deepcopy() {
    CreateTableAsSelectExecutionNode node = new CreateTableAsSelectExecutionNode(plan);
    copyFields(this, node);
    return node;
  }
  
  void copyFields(CreateTableAsSelectExecutionNode from, CreateTableAsSelectExecutionNode to) {
    super.copyFields(from, to);
    to.plan = from.plan;
  }

}
