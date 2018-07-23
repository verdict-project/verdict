package org.verdictdb.sqlsyntax;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Lists;

public class ImpalaSyntax extends SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public void dropTable(String schema, String tablename) {

  }
  
  @Override
  public Collection<String> getCandidateJDBCDriverClassNames() {
    List<String> candidates = Lists.newArrayList(
        "com.cloudera.impala.jdbc41.Driver");
    return candidates;
  }

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }
  
  @Override
  public String getFallbackDefaultSchema() {
    return "default";
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "DESCRIBE " + quoteName(schema) + "." + quoteName(table);
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getPartitionByInCreateTable() {
    return "partitioned by";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "SHOW PARTITIONS " + quoteName(schema) + "." + quoteName(table);
  }

  @Override
  public String getQuoteString() {
    return "`";
  }

  @Override
  public String getSchemaCommand() {
    return "SHOW DATABASES";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    return "SHOW TABLES IN " + quoteName(schema);
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public String randFunction() {
    int randomNum = ThreadLocalRandom.current().nextInt(0, (int) 1e9);
    // 1. unix_timestamp() prevents the same random numbers are generated for different column
    // especially when "create table as select" is used.
    // 2. adding a random number to the timestamp prevents the same random numbers are generated
    // for different columns.
    return String.format("rand(unix_timestamp()+%d)", randomNum);
  }
  
  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }
  
}
