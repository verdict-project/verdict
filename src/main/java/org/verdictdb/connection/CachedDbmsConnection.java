/*
 *    Copyright 2017 University of Michigan
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.verdictdb.connection;

import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.sqlsyntax.SqlSyntax;

/**
 * Offers the same functionality as DbmsConnection; however, returns cached metadata whenever
 * possible to speed up query processing.
 *
 * @author Yongjoo Park
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
