package org.verdictdb.core.logical_query;

import org.verdictdb.exception.UnexpectedCallException;


public class BaseTable implements AbstractRelation {

    public String toSql() {
        throw UnexpectedCallException("A base table itself cannot be converted to a sql.");
    }
}
