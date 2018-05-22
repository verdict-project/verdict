package org.verdictdb.core.logical_query;

public class RelationalOp implements AbstractRelation {
    
    AbstractRelation sourceRelation;
    
    /**
     * One of the following operations:
     * <ol>
     * <li>project</li>
     * <li>filter</li>
     * <li>aggregate</li>
     * </ol>
     */
    String opType;
    
    /**
     * Depending on the opType, this field has different roles:
     * <ol>
     * <li>project: projection column list</li>
     * <li>filter: a predicate</li>
     * <li>aggregate: aggregate column list + groupby list</li>
     * </ol>
     */
    Object parameters;
    
    public RelationalOp(AbstractRelation sourceRelation, String opType, Object parameters) {
        this.sourceRelation = sourceRelation;
        this.opType = opType;
        this.parameters = parameters;
    }

    public AbstractRelation getSourceRelation() {
        return sourceRelation;
    }

    public void setSourceRelation(AbstractRelation sourceRelation) {
        this.sourceRelation = sourceRelation;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public Object getParameters() {
        return parameters;
    }

    public void setParameters(Object parameters) {
        this.parameters = parameters;
    }

}
