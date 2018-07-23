package org.verdictdb.connection;

import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;

/**
 * Offers the same functionality as DbmsConnection; however, returns cached metadata whenever
 * possible to speed up query processing.
 * 
 * @author Yongjoo Park
 *
 */
public class CachedDbmsConnection extends CachedMetaDataProvider implements DbmsConnection {
  
  DbmsConnection originalConn;
  
  public CachedDbmsConnection(DbmsConnection conn) {
    super(conn);
    this.originalConn = conn; 
  }

  @Override
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException {
    return originalConn.execute(query);
  }

  @Override
  public SqlSyntax getSyntax() {
    return originalConn.getSyntax();
  }

  @Override
  public void close() {
    originalConn.close();
  }
  
  public DbmsConnection getOriginalConnection() {
    return originalConn;
  }

  @Override
  public DbmsConnection copy() {
    CachedDbmsConnection newConn = new CachedDbmsConnection(originalConn.copy());
    return newConn;
  }

}
