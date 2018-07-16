package org.verdictdb.sqlwriter;

import java.util.List;

import org.verdictdb.core.sqlobject.CreateTable;
import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.CreateTableStatement;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class CreateTableToSql {

  SqlSyntax syntax;
  
  public CreateTableToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }
  
  public String toSql(CreateTable query) throws VerdictDBException {
    String sql;
    if (query instanceof CreateTableAsSelectQuery) {
      sql = createAsSelectQueryToSql((CreateTableAsSelectQuery) query);
    } else if (query instanceof CreateTableStatement) {
      sql = createTableToSql((CreateTableStatement) query);
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
    sql.append("CREATE TABLE ");
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
      sql.append(" AS ");
    } else {
      sql.append(" ");
    }
    SelectQueryToSql selectWriter = new SelectQueryToSql(syntax);
    String selectSql = selectWriter.toSql(select);
    sql.append(selectSql);
  
    return sql.toString();
  }

  String createTableToSql(CreateTableStatement query) {
    // TODO Auto-generated method stub
    return null;
  }

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }

}
