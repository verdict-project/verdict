package org.verdictdb.core.logical_query;

import java.util.Arrays;
import java.util.List;


public class ColumnOp implements AbstractColumn {
    
    List<AbstractColumn> operands;
    
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
    
    public ColumnOp(String opType) {
        this.opType = opType;
    }
    
    public ColumnOp(String opType, AbstractColumn operand) {
        this.operands = Arrays.asList(operand);
        this.opType = opType;
    }
    
    public ColumnOp(String opType, List<AbstractColumn> operands) {
        this.operands = operands;
        this.opType = opType;
    }

    public AbstractColumn getOperand() {
        return getOperand(0);
    }
    
    public AbstractColumn getOperand(int i) {
        return operands.get(i);
    }

    public void setOperand(List<AbstractColumn> operands) {
        this.operands = operands;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

}
