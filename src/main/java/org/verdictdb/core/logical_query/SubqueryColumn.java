/*
    Subquery that may appeared in the select clause, where clause
 */
package org.verdictdb.core.logical_query;

public class SubqueryColumn implements UnnamedColumn {
    SelectQueryOp subquery = new SelectQueryOp();

    public SubqueryColumn() {}

    public SubqueryColumn(SelectQueryOp relation) {
        subquery = relation;
    }

    public static SubqueryColumn getSubqueryColumn(SelectQueryOp relation) {
        return new SubqueryColumn(relation);
    }

    public SelectQueryOp getSubquery() {
        return subquery;
    }
}
