package org.verdictdb.sqlwriter;

import java.util.List;

import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SparkSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class CreateTableToSql {

  SqlSyntax syntax;

  public CreateTableToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(CreateTableAsSelectQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    SelectQuery select = query.getSelect();

    // table
    sql.append("create table ");
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
    } else if (syntax instanceof SparkSyntax) {
      sql.append(" using parquet ");
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

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }

}
