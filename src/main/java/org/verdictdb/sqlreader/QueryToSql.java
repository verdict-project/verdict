package org.verdictdb.sqlreader;

import org.verdictdb.core.sqlobject.CreateTableAsSelectQuery;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class QueryToSql {
  
  public static String convert(SqlSyntax syntax, SqlConvertable query) throws VerdictDBException {
    if (query instanceof SelectQuery) {
      SelectQueryToSql tosql = new SelectQueryToSql(syntax);
      return tosql.toSql((SelectQuery) query);
    }
    else if (query instanceof CreateTableAsSelectQuery) {
      CreateTableToSql tosql = new CreateTableToSql(syntax);
      return tosql.toSql((CreateTableAsSelectQuery) query);
    }
    else if (query instanceof DropTableQuery) {
      DropTableToSql tosql = new DropTableToSql(syntax);
      return tosql.toSql((DropTableQuery) query);
    }
    else {
      throw new VerdictDBTypeException(query);
    }
  }

}
