package org.verdictdb.sqlwriter;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.CreateTableDefinitionQuery;
import org.verdictdb.core.sqlobject.CreateTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
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
    } else {
      throw new VerdictDBTypeException(query);
    }
    return sql;
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
