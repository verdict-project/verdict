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

import java.util.Collection;
import java.util.List;

import com.google.common.collect.Lists;

public class H2Syntax extends SqlSyntax {

  @Override
  public Collection<String> getCandidateJDBCDriverClassNames() {
    List<String> candidates = Lists.newArrayList("org.h2.Driver");
    return candidates;
  }

  // The column index that stored meta information in the original database
  @Override
  public int getSchemaNameColumnIndex() {
    return 0;
  }

  @Override
  public int getTableNameColumnIndex() {
    return 0;
  }

  @Override
  public int getColumnNameColumnIndex() {
    return 0;
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  @Override
  public String getQuoteString() {
    return "\"";
  }

  @Override
  public void dropTable(String schema, String tablename) {}

  @Override
  public boolean doesSupportTablePartitioning() {
    return false;
  }

  @Override
  public String randFunction() {
    return "rand()";
  }

  @Override
  public String getSchemaCommand() {
    return "show schemas";
  }

  @Override
  public String getTableCommand(String schema) {
    return "show tables from " + quoteName(schema);
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "show columns from " + quoteName(table) + " from " + quoteName(schema);
  }

  /** H2 does not support partitioning. */
  @Override
  public String getPartitionCommand(String schema, String table) {
    return null;
  }

  /** H2 does not support partitioning. */
  @Override
  public String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts) {
    return null;
  }

  @Override
  public boolean isAsRequiredBeforeSelectInCreateTable() {
    return true;
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
