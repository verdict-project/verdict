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

package org.verdictdb.sqlwriter;

import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class DropTableToSql {

  SqlSyntax syntax;

  public DropTableToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(DropTableQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();

    // table
    sql.append("drop table ");
    if (query.isIfExists()) {
      sql.append("if exists ");
    }
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));

    return sql.toString();
  }

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }
}
