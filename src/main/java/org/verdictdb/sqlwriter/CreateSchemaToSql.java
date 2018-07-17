package org.verdictdb.sqlwriter;

import org.verdictdb.core.querying.CreateSchemaQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class CreateSchemaToSql {
  
protected SqlSyntax syntax;
  
  public CreateSchemaToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }
  
  public String toSql(CreateSchemaQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();
    sql.append("create schema ");
    if (query.isIfNotExists()) {
      sql.append("if not exists ");
    }
    sql.append(query.getSchemaName());
    return sql.toString();
    
  }

}
