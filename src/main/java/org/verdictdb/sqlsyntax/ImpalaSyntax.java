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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Lists;

public class ImpalaSyntax extends SqlSyntax {

  private static final Map<String, String> typeMap;

  static {
    typeMap = new HashMap<>();
    typeMap.put("text", "string");
  }

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public void dropTable(String schema, String tablename) {}

  @Override
  public Collection<String> getCandidateJDBCDriverClassNames() {
    List<String> candidates = Lists.newArrayList("com.cloudera.impala.jdbc41.Driver");
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
  public String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts) {
    String sql = new HiveSyntax().getPartitionByInCreateTable(partitionColumns, partitionCounts);
    return sql;
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

  @Override
  public String substituteTypeName(String type) {
    String newType = typeMap.get(type.toLowerCase());
    return (newType != null) ? newType : type;
  }

  @Override
  public String getGenericStringDataTypeName() {
    return "STRING";
  }

  @Override
  public String getApproximateCountDistinct(String column) {
    return String.format("ndv(%s)", column);
  }

  /**
   * The following query returns 4.594682917363407 (4.59 / 100 = 0.0459):
   * 
   * select stddev(c)
   * from (
   *     select v, count(*) as c
   *     from (
   *         select pmod(fnv_hash(value), 100) as v
   *         from mytable2
   *     ) t1
   *     group by v
   * ) t2;
   * 
   * where mytable2 contains the integers from 0 to 10000.
   * 
   * Note that the stddev of rand() is sqrt(0.01 * 0.99) = 0.09949874371.
   */
  @Override
  public String hashFunction(String column) {
    String f = String.format(
        "pmod(fnv_hash(%s), %d) / %d",
        column, hashPrecision, hashPrecision);
    return f;
  }
}
