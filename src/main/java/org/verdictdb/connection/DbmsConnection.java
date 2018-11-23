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

import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBDbmsException;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.MysqlSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;
import org.verdictdb.sqlwriter.QueryToSql;

import java.util.ArrayList;
import java.util.List;

public abstract class DbmsConnection implements MetaDataProvider {

  /**
   * Executes a query (or queries). If the result exists, return it.
   *
   * <p>If a query includes multiple queries separated by semicolons, issue them separately in
   * order.
   *
   * @param sql
   * @return
   * @throws VerdictDBDbmsException
   */
  public abstract DbmsQueryResult execute(String sql) throws VerdictDBDbmsException;

  public DbmsQueryResult execute(SqlConvertible query) throws VerdictDBException {
    String sql = QueryToSql.convert(getSyntax(), query);
    DbmsQueryResult result = execute(sql);
    return result;
  }

  //  /**
  //   *
  //   * @param sql
  //   * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2)
  // 0 for
  //   * SQL statements that return nothing
  //   */
  //  public int executeUpdate(String query) throws VerdictDBDbmsException;

  public abstract SqlSyntax getSyntax();

  //  public Connection getConnection();

  public abstract void close();

  public abstract void abort();

  public abstract DbmsConnection copy() throws VerdictDBDbmsException;

  /**
   * @return a list of column names of primary key columns. (0-indexed)
   */
  public List<String> getPrimaryKey(String schema, String table) throws VerdictDBDbmsException {
    List<Integer> primaryKeyIndexList = new ArrayList<>();
    List<String> primaryKeyColumnName = new ArrayList<>();
    SqlSyntax syntax = getSyntax();
    if (syntax.getPrimaryKey(schema, table)!=null) {
      DbmsQueryResult result = execute(syntax.getPrimaryKey(schema, table));
      if (syntax instanceof MysqlSyntax) {
        while (result.next()) {
          primaryKeyIndexList.add(result.getInt(3) - 1);
        }
      }
      List<String> columns = new ArrayList<>();
      result = execute(syntax.getColumnsCommand(schema, table));
      while (result.next()) {
        columns.add(result.getString(0));
      }
      for (int idx:primaryKeyIndexList) {
        primaryKeyColumnName.add(columns.get(idx));
      }
    }

    return primaryKeyColumnName;
  }

}
