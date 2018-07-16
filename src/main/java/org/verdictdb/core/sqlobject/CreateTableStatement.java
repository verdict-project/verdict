package org.verdictdb.core.sqlobject;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Not implemented yet.
 * 
 * @author Yongjoo Park
 *
 */
public class CreateTableStatement extends CreateTable {

  private static final long serialVersionUID = -3733162210722527846L;

  String schemaName;

  String tableName;
  
  boolean ifNotExists = false;

  List<String> partitionColumns = new ArrayList<>();
  
  // See DataTypeConverter for types
  List<Pair<String, String>> columnNameAndTypes = new ArrayList<>();

  public String getSchemaName() {
    return schemaName;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<Pair<String, String>> getColumnNameAndTypes() {
    return columnNameAndTypes;
  }

  public void setSchemaName(String schemaName) {
    this.schemaName = schemaName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public void setPartitionColumns(List<String> partitionColumns) {
    this.partitionColumns = partitionColumns;
  }

  public void setColumnNameAndTypes(List<Pair<String, String>> columnNameAndTypes) {
    this.columnNameAndTypes = columnNameAndTypes;
  }
  
  public void addColumnNameAndType(Pair<String, String> nameAndType) {
    this.columnNameAndTypes.add(nameAndType);
  }

}
