package org.verdictdb.core.sql;

import org.verdictdb.core.query.CreateTableAsSelectQuery;
import org.verdictdb.core.query.DropTableQuery;
import org.verdictdb.core.query.SelectQuery;
import org.verdictdb.core.query.SqlConvertable;
import org.verdictdb.exception.UnexpectedTypeException;
import org.verdictdb.exception.VerdictDbException;
import org.verdictdb.sql.syntax.SyntaxAbstract;

public class QueryToSql {
  
  public static String convert(SyntaxAbstract syntax, SqlConvertable query) throws VerdictDbException {
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
      throw new UnexpectedTypeException(query);
    }
  }

}
