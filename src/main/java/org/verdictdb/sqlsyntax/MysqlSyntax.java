/*
 *    Copyright 2018 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.sqlsyntax;

public class MysqlSyntax extends SqlSyntax {

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public void dropTable(String schema, String tablename) {}

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public String getFallbackDefaultSchema() {
    return "test";
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "show columns in " + quoteName(table) + " in " + quoteName(schema);
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getPartitionByInCreateTable() {
    return "partition by key";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "SELECT DISTINCT REPLACE(PARTITION_EXPRESSION, '`', '') FROM INFORMATION_SCHEMA.PARTITIONS "
        + "WHERE TABLE_NAME='"
        + table
        + "' AND TABLE_SCHEMA='"
        + schema
        + "'";
  }

  @Override
  public String getQuoteString() {
    // https://dev.mysql.com/doc/refman/8.0/en/identifiers.html
    return "`";
  }

  @Override
  public String getSchemaCommand() {
    return "show schemas";
  }

  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public String getTableCommand(String schema) {
    return "show tables in " + quoteName(schema);
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return false;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (obj.getClass() != getClass()) {
      return false;
    }
    return true;
  }
}
