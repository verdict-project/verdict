package org.verdictdb.core.logical_query;

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
public class CreateTableAsSelect {
  
  String tableName;
  
  SelectQueryOp select;
  
  List<String> partitionColumns = new ArrayList<>();
  
  public CreateTableAsSelect(String tableName, SelectQueryOp select) {
    this.tableName = tableName;
    this.select = select;
  }

  public String getTableName() {
    return tableName;
  }

  public SelectQueryOp getSelect() {
    return select;
  }
  
  public void addPartitionColumn(String column) {
    partitionColumns.add(column);
  }
  
  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

}
