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

public class PostgresqlSyntax extends SqlSyntax {

  public static final String CHILD_PARTITION_TABLE_SUFFIX = "_vpart%05d";

  @Override
  public Collection<String> getCandidateJDBCDriverClassNames() {
    List<String> candidates = Lists.newArrayList("org.postgresql.Driver");
    return candidates;
  }

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
  public String getFallbackDefaultSchema() {
    return "public";
  }

  @Override
  public int getColumnTypeColumnIndex() {
    return 1;
  }

  public int getCharacterMaximumLengthColumnIndex() {
    return 2;
  }

  @Override
  public String getQuoteString() {
    return "\"";
  }

  @Override
  public void dropTable(String schema, String tablename) {}

  @Override
  public boolean doesSupportTablePartitioning() {
    return true;
  }

  @Override
  public String randFunction() {
    return "random()";
  }

  @Override
  public String getSchemaCommand() {
    return "select schema_name from information_schema.schemata";
  }

  @Override
  public String getTableCommand(String schema) {
    return "SELECT table_name FROM information_schema.tables "
        + "WHERE table_schema = '"
        + schema
        + "'";
  }

  @Override
  public String getColumnsCommand(String schema, String table) {
    return "select column_name, data_type, character_maximum_length "
        + "from INFORMATION_SCHEMA.COLUMNS where table_name = '"
        + table
        + "' and table_schema = '"
        + schema
        + "'";
  }

  @Override
  public String getPartitionCommand(String schema, String table) {
    return "select partattrs from pg_partitioned_table join pg_class on pg_class.relname='"
        + table
        + "' "
        + "and pg_class.oid = pg_partitioned_table.partrelid join information_schema.tables "
        + "on table_schema='"
        + schema
        + "' and table_name = '"
        + table
        + "'";
  }

  @Override
  public String getPartitionByInCreateTable(
      List<String> partitionColumns, List<Integer> partitionCounts) {
    return "PARTITION BY LIST";
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

  /**
   * The following query returns 9.7244874705897259 (9.72 / 100 = 0.0972):
   * 
   * select stddev(c)
   * from (
   *     select v, count(*) as c
   *     from (
   *         select ('x' || lpad(substr(md5(cast(value as varchar)), 1, 8), 16, '0'))::bit(64)::bigint % 100 as v
   *         from mytable
   *     ) t1
   *     group by v
   * ) t2;
   * 
   * where mytable contains the integers from 0 to 10000.
   * 
   * Note that the stddev of rand() is sqrt(0.01 * 0.99) = 0.09949874371.
   * 
   * PostreSQL's hex to int conversion is described at 
   * https://stackoverflow.com/questions/8316164/convert-hex-in-text-representation-to-decimal-number
   */
  @Override
  public String hashFunction(String column, int upper_bound) {
    String f = String.format(
        "('x' || lpad(substr(md5(cast(%s%s%s as varchar)), 1, 8), 16, '0'))::bit(64)::bigint % %d",
        getQuoteString(), column, getQuoteString(), upper_bound);
    return f;
  }
}
