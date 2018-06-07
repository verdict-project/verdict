package org.verdictdb.core.sql;

import org.verdictdb.core.query.CreateTableAsSelect;
import org.verdictdb.core.query.SelectQueryOp;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class LogicalCreateTableToSql {

  SyntaxAbstract syntax;

  public LogicalCreateTableToSql(SyntaxAbstract syntax) {
    this.syntax = syntax;
  }

  public String toSql(CreateTableAsSelect query) throws VerdictDbException {
    StringBuilder sql = new StringBuilder();

    String tableName = query.getTableName();
    SelectQueryOp select = query.getSelect();

    // table
    sql.append("create table ");
    sql.append(tableName);
    sql.append(" as ");

    // select
    SelectQueryToSql selectWriter = new SelectQueryToSql(syntax);
    String selectSql = selectWriter.toSql(select);
    sql.append(selectSql);

    return sql.toString();
  }

}
