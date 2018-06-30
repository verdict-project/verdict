package org.verdictdb;

import org.verdictdb.core.connection.JdbcConnection;
import org.verdictdb.resulthandler.AsyncHandler;
import org.verdictdb.sql.syntax.SqlSyntax;
import org.verdictdb.sql.syntax.SyntaxReader;

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
  
  public void asyncExecuteQuery(String query, AsyncHandler handler) {
    
  }

}
