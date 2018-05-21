package org.verdictdb.core.logical_query;

import org.verdictdb.core.sql.syntax.SyntaxAbstract;
import org.verdictdb.exception.VerdictDbException;

public class ColumnOp implements AbstractColumn {
    
    AbstractColumn source;
    
    /**
     * opType must be one of the following:
     * <ol>
     * <li>sum</li>
     * <li>count</li>
     * <li>avg</li>
     * <li>add</li>
     * <li>multiply</li>
     * <li>subtract</li>
     * <li>divide</li>
     * </ol>
     */
    String opType;
    
    public ColumnOp(AbstractColumn source, String opType) {
        this.source = source;
        this.opType = opType;
    }
    
    public String toSql(SyntaxAbstract syntax) throws VerdictDbException {
        return opType + "(" + source.toSql(syntax) + ")";
    }

}
