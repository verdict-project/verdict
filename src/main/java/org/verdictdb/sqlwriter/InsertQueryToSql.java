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

import com.google.common.base.Joiner;
import org.verdictdb.commons.VerdictTimestamp;
import org.verdictdb.core.sqlobject.InsertIntoSelectQuery;
import org.verdictdb.core.sqlobject.InsertValuesQuery;
import org.verdictdb.core.sqlobject.SelectItem;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.sqlsyntax.PrestoSyntax;
import org.verdictdb.sqlsyntax.SqlSyntax;

import java.util.ArrayList;
import java.util.List;

public class InsertQueryToSql {

  SqlSyntax syntax;

  public InsertQueryToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(InsertIntoSelectQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();
    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();

    // table
    sql.append("insert into ");
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));

    SelectQueryToSql selectWriter = new SelectQueryToSql(syntax);
    List<String> selectItemStrList = new ArrayList<>();
    for (SelectItem item : query.getSelectQuery().getSelectList()) {
      selectItemStrList.add(selectWriter.selectItemToSqlPart(item));
    }

    if (!(selectItemStrList.size() == 1 && selectItemStrList.get(0).equals("*"))) {
      sql.append("(");
      sql.append(Joiner.on(",").join(selectItemStrList));
      sql.append(")");
    }

    sql.append(" ");

    // select
    String selectSql = selectWriter.toSql(query.getSelectQuery());
    sql.append(selectSql);

    return sql.toString();
  }

  public String toSql(InsertValuesQuery query) throws VerdictDBException {
    StringBuilder sql = new StringBuilder();

    String schemaName = query.getSchemaName();
    String tableName = query.getTableName();
    List<Object> values = query.getValues();

    // table
    sql.append("insert into ");
    sql.append(quoteName(schemaName));
    sql.append(".");
    sql.append(quoteName(tableName));

    // values
    sql.append(" values (");
    boolean isFirst = true;
    for (Object v : values) {
      if (isFirst == false) {
        sql.append(", ");
      }
      if (v instanceof VerdictTimestamp) {
        if (syntax instanceof PrestoSyntax) {
          sql.append("timestamp '" + v.toString() + "'");
        } else {
          sql.append("'" + v.toString() + "'");
        }
      } else if (v instanceof String) {
        sql.append("'" + v + "'");
      } else {
        sql.append(v.toString());
      }
      isFirst = false;
    }
    sql.append(")");

    return sql.toString();
  }

  String quoteName(String name) {
    String quoteString = syntax.getQuoteString();
    return quoteString + name + quoteString;
  }
}
