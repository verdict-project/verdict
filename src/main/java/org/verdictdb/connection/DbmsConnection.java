/*
 *    Copyright 2018 University of Michigan
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

public interface DbmsConnection extends MetaDataProvider {

  /**
   * Executes a query (or queries). If the result exists, return it.
   *
   * <p>If a query includes multiple queries separated by semicolons, issue them separately in
   * order.
   *
   * @param query
   * @return
   * @throws VerdictDBDbmsException
   */
  public DbmsQueryResult execute(String query) throws VerdictDBDbmsException;

  //  /**
  //   *
  //   * @param sql
  //   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2)
  // 0 for
  //   * SQL statements that return nothing
  //   */
  //  public int executeUpdate(String query) throws VerdictDBDbmsException;

  public SqlSyntax getSyntax();

  //  public Connection getConnection();

  public void close();

  public DbmsConnection copy();
}
