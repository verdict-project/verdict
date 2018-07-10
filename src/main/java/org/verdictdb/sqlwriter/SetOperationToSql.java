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
        + " " + ((SetOperationRelation) relation).getSetOpType()
        + " " + selectQueryToSql.relationToSqlPart(((SetOperationRelation) relation).getRight());
  }


}
