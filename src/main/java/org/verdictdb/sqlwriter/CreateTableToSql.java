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

package org.verdictdb.sqlwriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.commons.VerdictDBLogger;
import org.verdictdb.core.sqlobject.AsteriskColumn;
import org.verdictdb.core.sqlobject.BaseTable;
import org.verdictdb.core.sqlobject.CreateScrambledTableQuery;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.CreateTableDefinitionQuery;
import org.verdictdb.core.sqlobject.CreateTableQuery;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlsyntax.PrestoSyntax;
import org.verdictdb.sqlsyntax.ImpalaSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

import com.google.common.base.Joiner;

public class CreateTableToSql {

  protected SqlSyntax syntax;

  public CreateTableToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(CreateTableQuery query) throws VerdictDBException {
    VerdictDBLogger logger = VerdictDBLogger.getLogger(this.getClass());
    logger.debug("Converting the following sql object to string: " + query);
    
    String sql;
    if (query instanceof CreateTableAsSelectQuery) {
      sql = createAsSelectQueryToSql((CreateTableAsSelectQuery) query);
    } else if (query instanceof CreateTableDefinitionQuery) {
      sql = createTableToSql((CreateTableDefinitionQuery) query);
    } else if (query instanceof CreateScrambledTableQuery) {
      if (syntax instanceof PostgresqlSyntax) {
        sql = createPostgresqlPartitionTableToSql((CreateScrambledTableQuery) query);
      } else if (syntax instanceof ImpalaSyntax) {
        sql = createImpalaPartitionTableToSql((CreateScrambledTableQuery) query);
      } else {
        sql =
            createAsSelectQueryToSql(
                new CreateTableAsSelectQuery((CreateScrambledTableQuery) query));
      }
    } else {
      throw new VerdictDBTypeException(query);
    }
    return sql;
  }

  private String createImpalaPartitionTableToSql(CreateScrambledTableQuery query)
      throws VerdictDBException {

    // 1. This method should only get called when the target DB is Impala.
    // 2. Currently, Impala's create-table-as-select has a bug; dynamic partitioning is faulty
    // when used in conjunction with rand();
    if (!(syntax instanceof ImpalaSyntax)) {
      throw new VerdictDBException("Target database must be Impala.");
    }

    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    SelectQuery select = query.getSelect();

    // this table will be created and dropped at the end
    int randomNum = ThreadLocalRandom.current().nextInt(0, 10000);
    String tempTableName = "verdictdb_scrambling_temp_" + randomNum;

    // create a non-partitioned temp table as a select
    CreateTableAsSelectQuery tempCreate =
        new CreateTableAsSelectQuery(schemaName, tempTableName, select);
    sql.append(QueryToSql.convert(syntax, tempCreate));
    sql.append(";");

    // insert the temp table into a partitioned table.
    String aliasName = "t";
    SelectQuery selectAllFromTemp =
        SelectQuery.create(
            new AsteriskColumn(), new BaseTable(schemaName, tempTableName, aliasName));
    CreateTableAsSelectQuery insert =
        new CreateTableAsSelectQuery(schemaName, tableName, selectAllFromTemp);
    for (String col : query.getPartitionColumns()) {
      insert.addPartitionColumn(col);
    }
    sql.append(QueryToSql.convert(syntax, insert));
    sql.append(";");

    // drop the temp table
    DropTableQuery drop = new DropTableQuery(schemaName, tempTableName);
    sql.append(QueryToSql.convert(syntax, drop));
    sql.append(";");

    return sql.toString();
  }

  private String createPostgresqlPartitionTableToSql(CreateScrambledTableQuery query)
      throws VerdictDBException {

    // 1. This method should only get called when the target DB is postgres.
    // 2. Currently, partition tables in postgres must have a single partition column as we use
    // 'partition by list'.
    if (!(syntax instanceof PostgresqlSyntax)) {
      throw new VerdictDBException("Target database must be Postgres.");
    } else if (query.getPartitionColumns().size() != 1) {
      throw new VerdictDBException(
          "Scrambled tables must have a single partition column in Postgres.");
    }

    StringBuilder sql = new StringBuilder();

    int blockCount = query.getBlockCount();
    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    SelectQuery select = query.getSelect();

    // table
    sql.append("create table ");
    if (query.isIfNotExists()) {
      sql.append("if not exists ");
    }
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));
    sql.append(" (");
    List<String> columns = new ArrayList<>();
    for (Pair<String, String> col : query.getColumnMeta()) {
      columns.add(col.getLeft() + " " + col.getRight());
    }
    sql.append(Joiner.on(",").join(columns));
    sql.append(
        String.format(
            ", %s integer, %s integer", query.getTierColumnName(), query.getBlockColumnName()));
    sql.append(")");

    // partitions
    sql.append(" ");
    sql.append(syntax.getPartitionByInCreateTable(
        query.getPartitionColumns(), Arrays.<Integer>asList(query.getBlockCount())));
    sql.append(" (");

    // only single column for partition
    String partitionColumn = query.getPartitionColumns().get(0);
    sql.append(quoteName(partitionColumn));
    sql.append(")");
    sql.append("; ");

    // create child partition tables for postgres
    for (int blockNum = 0; blockNum < blockCount; ++blockNum) {
      sql.append("create table ");
      if (query.isIfNotExists()) {
        sql.append("if not exists ");
      }
      sql.append(quoteName(schemaName));
      sql.append(".");
      sql.append(
          quoteName(
              String.format(
                  "%s" + PostgresqlSyntax.CHILD_PARTITION_TABLE_SUFFIX, tableName, blockNum)));
      sql.append(
          String.format(
              " partition of %s.%s for values in (%d); ",
              quoteName(schemaName), quoteName(tableName), blockNum));
    }

    sql.append("insert into ");
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));
    sql.append(" ");

    // select
    SelectQueryToSql selectWriter = new SelectQueryToSql(syntax);
    String selectSql = selectWriter.toSql(select);
    sql.append(selectSql);

    return sql.toString();
  }

  String createAsSelectQueryToSql(CreateTableAsSelectQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    SelectQuery select = query.getSelect();

    // table
    sql.append("create table ");
    if (query.isIfNotExists()) {
      sql.append("if not exists ");
    }
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));
    sql.append(" ");
    
    // parquet format for Spark
    if (syntax instanceof SparkSyntax) {
      sql.append("using parquet ");
    }

    // partitions
    if (syntax.doesSupportTablePartitioning() && query.getPartitionColumns().size() > 0) {
      sql.append(
          syntax.getPartitionByInCreateTable(
              query.getPartitionColumns(), query.getPartitionCounts()));
      sql.append(" ");
    }
    
    // parquet format
    if (syntax instanceof HiveSyntax || syntax instanceof ImpalaSyntax || syntax instanceof PrestoSyntax) {
      sql.append("stored as parquet ");
    }

    // select
    if (syntax.isAsRequiredBeforeSelectInCreateTable()) {
      sql.append("as ");
    }
    SelectQueryToSql selectWriter = new SelectQueryToSql(syntax);
    String selectSql = selectWriter.toSql(select);
    sql.append(selectSql);

    return sql.toString();
  }

  String createTableToSql(CreateTableDefinitionQuery query) {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    List<Pair<String, String>> columnAndTypes = query.getColumnNameAndTypes();

    // table
    sql.append("create table ");
    if (query.isIfNotExists()) {
      sql.append("if not exists ");
    }
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));

    // column definitions
    sql.append(" (");
    boolean isFirst = true;
    for (Pair<String, String> columnAndType : columnAndTypes) {
      String column = columnAndType.getLeft();
      String type = columnAndType.getRight();
      type = syntax.substituteTypeName(type);
      if (isFirst == false) {
        sql.append(", ");
      }
      sql.append(String.format("%s %s", quoteName(column), type));
      isFirst = false;
    }
    sql.append(")");

    return sql.toString();
  }

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }
}
