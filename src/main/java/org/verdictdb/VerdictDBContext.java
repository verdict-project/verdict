package org.verdictdb;

import org.verdictdb.core.connection.DbmsConnection;
import org.verdictdb.core.connection.DbmsQueryResult;
import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlsyntax.SyntaxReader;

public class VerdictDBContext {
  
  DbmsConnection conn;
  
//  DbmsMetadataCache meta = new DbmsMetadataCache();
  
  public VerdictDBContext(DbmsConnection conn) {
    this.conn = conn;
  }
  
  public static VerdictDBContext create(java.sql.Connection jdbcConn) {
    SqlSyntax syntax = SyntaxReader.infer(jdbcConn);
    VerdictDBContext vc = new VerdictDBContext(new JdbcConnection(jdbcConn, syntax));
    return vc;
  }
  
  public DbmsQueryResult sql(String query) {
    return null;
  }

}
