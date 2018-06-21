package org.verdictdb.core.sql;

import java.util.List;

import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class CreateTableToSql {

  SyntaxAbstract syntax;

  public CreateTableToSql(SyntaxAbstract syntax) {
    this.syntax = syntax;
  }

  public String toSql(CreateTableAsSelectQuery query) throws VerdictDbException {
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
      sql.append(" partitioned by (");
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
    }

    // select
    sql.append(" as ");
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
