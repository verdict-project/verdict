package org.verdictdb.core.sql;

import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.SqlConvertable;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class QueryToSql {
  
  public static String convert(SyntaxAbstract syntax, SqlConvertable query) throws VerdictDBException {
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
