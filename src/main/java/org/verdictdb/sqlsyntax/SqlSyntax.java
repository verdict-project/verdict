package org.verdictdb.sqlsyntax;

import java.util.Collection;
import java.util.Collections;

public abstract class SqlSyntax {

  public abstract boolean doesSupportTablePartitioning();

  public abstract void dropTable(String schema, String tablename);

  // The column index that stored meta information in the original database
  public abstract int getColumnNameColumnIndex();

  public abstract String getColumnsCommand(String schema, String table);

  // The column index that stored meta information in the original database
  public abstract int getColumnTypeColumnIndex();

  public abstract String getPartitionByInCreateTable();

  public abstract String getPartitionCommand(String schema, String table);

  public abstract String getQuoteString();

  public abstract String getSchemaCommand();

  public abstract int getSchemaNameColumnIndex();

  public abstract String getTableCommand(String schema);

  // The column index that stored meta information in the original database
  public abstract int getTableNameColumnIndex();

  public abstract String randFunction();
  
  public abstract boolean isAsRequiredBeforeSelectInCreateTable();

  public String getStddevPopulationFunctionName() {
    return "stddev_pop";
  }
  
  public String quoteName(String name) {
    String quoteString = getQuoteString();
    return quoteString + name + quoteString;
  }

  public String substituteTypeName(String type) {
    return type;
  }
  
  /**
   * The drivers returned by methods are loaded explicitly by JdbcConnection (when it makes a 
   * JDBC connection to the backend database.) This mechanism is to support legacy library
   * that does not support automatic JDBC driver discovery.
   * @return
   */
  public Collection<String> getCandidateJDBCDriverClassNames() {
    return Collections.emptyList();
  }

  public String getFallbackDefaultSchema() {
    throw new RuntimeException("This function must be implemented for each dbms syntax.");
  }
}
