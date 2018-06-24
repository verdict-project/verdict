package org.verdictdb.core.query;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a create table query.
 * 
 * https://www.cloudera.com/documentation/enterprise/5-8-x/topics/impala_create_table.html
 * 
 * @author Yongjoo Park
 *
 */
public class CreateTableAsSelectQuery {
  
  String schemaName;
  
  String tableName;
  
  SelectQuery select;
  
  List<String> partitionColumns = new ArrayList<>();
  
  public CreateTableAsSelectQuery(String schemaName, String tableName, SelectQuery select) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.select = select;
  }
  
  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public SelectQuery getSelect() {
    return select;
  }
  
  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }
  
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

}
