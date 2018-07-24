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

package org.verdictdb.connection;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

import java.util.ArrayList;
import java.util.List;

public class SparkConnection implements DbmsConnection {

  SparkSession sc;

  SqlSyntax syntax;

  String currentSchema;

  public SparkConnection(SparkSession sc) {
    this.sc = sc;
    this.syntax = new SparkSyntax();
  }

  public SparkConnection(SparkSession sc, SqlSyntax syntax) {
    this.sc = sc;
    this.syntax = syntax;
  }

  @Override
  public List<String> getSchemas() throws VerdictDBDbmsException {
    List<String> schemas = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getSchemaCommand());
    while (queryResult.next()) {
      schemas.add((String) queryResult.getValue(syntax.getSchemaNameColumnIndex()));
    }
    return schemas;
  }

  @Override
  public List<String> getTables(String schema) throws VerdictDBDbmsException {
    List<String> tables = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getTableCommand(schema));
    while (queryResult.next()) {
      tables.add((String) queryResult.getValue(syntax.getTableNameColumnIndex()));
    }
    return tables;
  }

  @Override
  public List<Pair<String, String>> getColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<Pair<String, String>> columns = new ArrayList<>();
    DbmsQueryResult queryResult = execute(syntax.getColumnsCommand(schema, table));
    while (queryResult.next()) {
      String name = queryResult.getString(syntax.getColumnNameColumnIndex());
      String type = queryResult.getString(syntax.getColumnTypeColumnIndex());
      type = type.toLowerCase();

      // when there exists partitions in a table, this extra information will be returned.
      // we should ignore this.
      if (name.equalsIgnoreCase("# Partition Information")) {
        break;
      }

      columns.add(new ImmutablePair<>(name, type));
    }

    return columns;
  }

  @Override
  public List<String> getPartitionColumns(String schema, String table)
      throws VerdictDBDbmsException {
    List<String> partition = new ArrayList<>();

    DbmsQueryResult queryResult = execute(syntax.getPartitionCommand(schema, table));
    boolean hasPartitionInfoStarted = false;
    while (queryResult.next()) {
      String name = queryResult.getString(0);
      if (hasPartitionInfoStarted && (name.equalsIgnoreCase("# col_name") == false)) {
        partition.add(name);
      } else if (name.equalsIgnoreCase("# Partition Information")) {
        hasPartitionInfoStarted = true;
      }
    }

    return partition;
  }

  @Override
  public String getDefaultSchema() {
    return currentSchema;
  }

  @Override
  public void setDefaultSchema(String schema) {
    currentSchema = schema;
  }

  @Override
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException {
    try {
      // System.out.println("query to issue " + query);
      SparkQueryResult srs = null;
      Dataset<Row> result = sc.sql(query);
      if (result != null) {
        srs = new SparkQueryResult(result);
      }
      return srs;
    } catch (Exception e) {
      //      e.printStackTrace();
      throw new VerdictDBDbmsException(e.getMessage());
    }
  }

  @Override
  public SqlSyntax getSyntax() {
    return syntax;
  }

  @Override
  public void close() {
    try {
      this.sc.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public SparkSession getSparkSession() {
    return sc;
  }

  @Override
  public DbmsConnection copy() {
    SparkConnection newConn = new SparkConnection(sc, syntax);
    newConn.setDefaultSchema(currentSchema);
    return newConn;
  }
}
