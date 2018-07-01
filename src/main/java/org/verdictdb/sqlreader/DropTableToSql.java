package org.verdictdb.sqlreader;

import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class DropTableToSql {
  
  SqlSyntax syntax;

  public DropTableToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(DropTableQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();

    // table
    sql.append("drop table ");
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));
    
    return sql.toString();
  }
  
  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }

}
