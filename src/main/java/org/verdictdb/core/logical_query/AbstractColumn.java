package org.verdictdb.core.logical_query;

import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.VerdictDbException;

public interface AbstractColumn {

    public String toSql(SyntaxAbstract syntax) throws VerdictDbException;
    
}
