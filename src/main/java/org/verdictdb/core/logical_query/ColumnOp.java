package org.verdictdb.core.logical_query;

public class ColumnOp implements AbstractColumn {
    
    AbstractColumn source;
    
    /**
     * opType must be one of the following:
     * <ol>
     * <li>*</li>
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
    
    public ColumnOp(String opType, AbstractColumn source) {
        this.source = source;
        this.opType = opType;
    }

    public AbstractColumn getSource() {
        return source;
    }

    public void setSource(AbstractColumn source) {
        this.source = source;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

}
