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

import org.verdictdb.core.sqlobject.CreateSchemaQuery;
import org.verdictdb.core.sqlobject.CreateTableQuery;
import org.verdictdb.core.sqlobject.DropTableQuery;
import org.verdictdb.core.sqlobject.InsertIntoSelectQuery;
import org.verdictdb.core.sqlobject.InsertValuesQuery;
import org.verdictdb.core.sqlobject.SelectQuery;
import org.verdictdb.core.sqlobject.SetOperationRelation;
import org.verdictdb.core.sqlobject.SqlConvertible;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.exception.VerdictDBValueException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class QueryToSql {

  public static String convert(SqlSyntax syntax, SqlConvertible query) throws VerdictDBException {
    if (query == null) {
      throw new VerdictDBValueException("null value passed");
    }

    if (query instanceof SelectQuery) {
      SelectQueryToSql tosql = new SelectQueryToSql(syntax);
      return tosql.toSql((SelectQuery) query);
    } else if (query instanceof CreateSchemaQuery) {
      CreateSchemaToSql tosql = new CreateSchemaToSql(syntax);
      return tosql.toSql((CreateSchemaQuery) query);
    } else if (query instanceof CreateTableQuery) {
      CreateTableToSql tosql = new CreateTableToSql(syntax);
      return tosql.toSql((CreateTableQuery) query);
    } else if (query instanceof DropTableQuery) {
      DropTableToSql tosql = new DropTableToSql(syntax);
      return tosql.toSql((DropTableQuery) query);
    } else if (query instanceof InsertValuesQuery) {
      InsertQueryToSql tosql = new InsertQueryToSql(syntax);
      return tosql.toSql((InsertValuesQuery) query);
    } else if (query instanceof InsertIntoSelectQuery) {
      InsertQueryToSql tosql = new InsertQueryToSql(syntax);
      return tosql.toSql((InsertIntoSelectQuery) query);
    } else if (query instanceof SetOperationRelation) {
      SetOperationToSql tosql = new SetOperationToSql(syntax);
      return tosql.toSql((SetOperationRelation) query);
    } else {
      throw new VerdictDBTypeException(query);
    }
  }
}
