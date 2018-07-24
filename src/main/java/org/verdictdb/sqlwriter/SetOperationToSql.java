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

import org.verdictdb.core.sqlobject.AbstractRelation;
import org.verdictdb.core.sqlobject.SetOperationRelation;
import org.verdictdb.exception.VerdictDBException;
import org.verdictdb.exception.VerdictDBTypeException;
import org.verdictdb.sqlsyntax.SqlSyntax;

public class SetOperationToSql {

  SqlSyntax syntax;

  public SetOperationToSql(SqlSyntax syntax) {
    this.syntax = syntax;
  }

  public String toSql(AbstractRelation relation) throws VerdictDBException {
    if (!(relation instanceof SetOperationRelation)) {
      throw new VerdictDBTypeException("Unexpected type: " + relation.getClass().toString());
    }
    SelectQueryToSql selectQueryToSql = new SelectQueryToSql(syntax);
    return selectQueryToSql.relationToSqlPart(((SetOperationRelation) relation).getLeft())
        + " "
        + ((SetOperationRelation) relation).getSetOpType()
        + " "
        + selectQueryToSql.relationToSqlPart(((SetOperationRelation) relation).getRight());
  }
}
