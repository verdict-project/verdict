package org.verdictdb.core.logical_query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ColumnOp implements UnnamedColumn, SelectItem {
    
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
     * <li>pow</li>
     * <li>sqrt</li>
     * <li>and</li>
     * <li>or</li>
     * <li>equal</li>
     * <li>notequal</li>
     * <li>notgreaterthan</li>
     * <li>notlessthan</li>
     * <li>casewhenelse</li>
     * <li>not null</li>
     * </ol>
     */
    String opType;
    
    List<UnnamedColumn> operands = new ArrayList<>();

    public ColumnOp(String opType) {
        this.opType = opType;
    }
    
    public ColumnOp(String opType, UnnamedColumn operand) {
        this.operands = Arrays.asList(operand);
        this.opType = opType;
    }
    
    public ColumnOp(String opType, List<UnnamedColumn> operands) {
        this.operands = operands;
        this.opType = opType;
    }

    public UnnamedColumn getOperand() {
        return getOperand(0);
    }
    
    public UnnamedColumn getOperand(int i) {
        return operands.get(i);
    }
    
    public List<UnnamedColumn> getOperands() {
        return operands;
    }

    public void setOperand(List<UnnamedColumn> operands) {
        this.operands = operands;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }
    
    public static ColumnOp and(UnnamedColumn predicate1, UnnamedColumn predicate2) {
        return new ColumnOp("and", Arrays.asList(predicate1, predicate2));
    }
    
    public static ColumnOp equal(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("equal", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp notequal(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("notequal", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp notgreaterthan(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("notgreaterthan", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp notlessthan(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("notlessthan", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp add(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("add", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp multiply(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("multiply", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp divide(UnnamedColumn column1, UnnamedColumn column2) {
        return new ColumnOp("divide", Arrays.asList(column1, column2));
    }
    
    public static ColumnOp casewhenelse(UnnamedColumn columnIf, UnnamedColumn condition, UnnamedColumn columnElse) {
        return new ColumnOp("casewhenelse", Arrays.asList(columnIf, condition, columnElse));
    }
    
    public static ColumnOp notnull(UnnamedColumn column1) {
        return new ColumnOp("notnull", Arrays.asList(column1));
    }
    
    public static ColumnOp std(UnnamedColumn column1) {
        return new ColumnOp("std", Arrays.asList(column1));
    }
    
    public static ColumnOp sqrt(UnnamedColumn column1) {
        return new ColumnOp("sqrt", Arrays.asList(column1));
    }
    
    public static ColumnOp sum(UnnamedColumn column1) {
        return new ColumnOp("sum", Arrays.asList(column1));
    }
    
    public static ColumnOp pow(UnnamedColumn column1, UnnamedColumn column2) {
      return new ColumnOp("pow", Arrays.asList(column1, column2));
  }

    public static UnnamedColumn count() {
        return new ColumnOp("count");
    }

}
