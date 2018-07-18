package org.verdictdb.sqlwriter;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.*;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sqlsyntax.HiveSyntax;
import org.verdictdb.sqlsyntax.PostgresqlSyntax;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class CreateTableToSql {

  protected SqlSyntax syntax;

  public CreateTableToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(CreateTableQuery query) throws VerdictDBException {
    String sql;
    if (query instanceof CreateTableAsSelectQuery) {
      sql = createAsSelectQueryToSql((CreateTableAsSelectQuery) query);
    } else if (query instanceof CreateTableDefinitionQuery) {
      sql = createTableToSql((CreateTableDefinitionQuery) query);
    } else if (query instanceof CreateScrambledTableQuery) {
      sql = (syntax instanceof PostgresqlSyntax) ? createPartitionTableToSql((CreateScrambledTableQuery) query) :
        createAsSelectQueryToSql(new CreateTableAsSelectQuery((CreateScrambledTableQuery) query));
    } else {
      throw new VerdictDBTypeException(query);
    }
    return sql;
  }

  private String createPartitionTableToSql(CreateScrambledTableQuery query) throws VerdictDBException {

    // 1. This method should only get called when the target DB is postgres.
    // 2. Currently, partition tables in postgres must have a single partition column as we use 'partition by list'.
    if (!(syntax instanceof PostgresqlSyntax)) {
      throw new VerdictDBException("Target database must be Postgres.");
    } else if (query.getPartitionColumns().size() != 1) {
      throw new VerdictDBException("Scrambled tables must have a single partition column in Postgres.");
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
    sql.append(String.format(",%s integer,%s integer", query.getTierColumnName(), query.getBlockColumnName()));
    sql.append(")");

    // partitions
    sql.append(" ");
    sql.append(syntax.getPartitionByInCreateTable());
    sql.append(" (");

    // only single column for partition
    String partitionColumn = query.getPartitionColumns().get(0);
    sql.append(quoteName(partitionColumn));
    sql.append(")");
    if (syntax instanceof PostgresqlSyntax) {
      sql.append("; ");
      // create child partition tables for postgres
      for (int blockNum = 0; blockNum < blockCount; ++blockNum) {
        sql.append("create table ");
        if (query.isIfNotExists()) {
          sql.append("if not exists ");
        }
        sql.append(quoteName(schemaName));
        sql.append(".");
        sql.append(quoteName(String.format("%s" + PostgresqlSyntax.CHILD_PARTITION_TABLE_SUFFIX,
            tableName, blockNum)));
        sql.append(String.format(" partition of %s.%s for values in (%d); ",
            quoteName(schemaName), quoteName(tableName), blockNum));
      }
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

    // partitions
    if (syntax.doesSupportTablePartitioning() && query.getPartitionColumns().size() > 0) {
      sql.append(" ");
      sql.append(syntax.getPartitionByInCreateTable());
      sql.append(" (");
      List<String> partitionColumns = query.getPartitionColumns();
      boolean isFirstColumn = true;
      for (String col : partitionColumns) {
        if (isFirstColumn) {
          sql.append(quoteName(col));
          isFirstColumn = false;
        } else {
          sql.append(", " + quoteName(col));
        }
      }
      sql.append(")");
    } else if (syntax instanceof SparkSyntax || syntax instanceof HiveSyntax) {
      sql.append(" using parquet");
    }

    // select
    if (syntax.isAsRequiredBeforeSelectInCreateTable()) {
      sql.append(" as ");
    } else {
      sql.append(" ");
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
