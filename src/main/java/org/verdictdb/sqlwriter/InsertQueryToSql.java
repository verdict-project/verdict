package org.verdictdb.sqlwriter;

import java.util.List;

import org.verdictdb.core.sqlobject.InsertValuesQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class InsertQueryToSql {

  SqlSyntax syntax;

  public InsertQueryToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(InsertValuesQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    List<Object> values = query.getValues();

    // table
    sql.append("insert into ");
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));
    
    // values
    sql.append(" values (");
    boolean isFirst = true;
    for (Object v : values) {
      if (isFirst == false) {
        sql.append(", ");
      }
      if (v instanceof String) {
        sql.append("'" + v + "'");
      } else {
        sql.append(v.toString());
      }
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
