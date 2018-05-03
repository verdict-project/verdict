package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.LateralFunc;
import edu.umich.verdict.relation.expr.TableNameExpr;

public class ApproxLateralViewRelation extends ApproxRelation {
    
//    protected ApproxRelation source;
    
    protected LateralFunc lateralFunc;
    
    protected String tableAlias;
    
    protected String columnAlias;
    
//    public ApproxRelation getSource() {
//        return source;
//    }

    public LateralFunc getLateralFunc() {
        return lateralFunc;
    }

    public ApproxLateralViewRelation(VerdictContext vc, LateralFunc lateralFunc, String tableAlias, String columnAlias) {
        super(vc);
//        this.source = source;
        this.lateralFunc = lateralFunc;
        this.tableAlias = tableAlias;
        this.columnAlias = columnAlias;
        setAlias(tableAlias);
//        this.alias = source.getAlias() + "-" + lateralFunc.getTableAlias();
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation r = new LateralViewRelation(vc, lateralFunc, tableAlias, columnAlias);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    protected ExactRelation rewriteWithPartition() {
        ExactRelation r = new LateralViewRelation(vc, lateralFunc, tableAlias, columnAlias);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        return new ArrayList<Expr>();
    }

    @Override
    public double samplingProbability() {
        return 1.0;
    }

    @Override
    public double cost() {
        return 1.0;
    }

    @Override
    public Expr tupleProbabilityColumn() {
    	return new ConstantExpr(vc, 1.0);
    }

    @Override
    public Expr tableSamplingRatio() {
    	return new ConstantExpr(vc, 1.0);
    }

    @Override
    public String sampleType() {
        return "nosample";
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        return new ArrayList<String>();
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return new HashMap<TableUniqueName, String>();
    }

    @Override
    protected boolean doesIncludeSample() {
        return false;
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxLateralViewRelation) {
            if (lateralFunc.equals(((ApproxLateralViewRelation) o).getLateralFunc())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected String toStringWithIndent(String indent) {
    	StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("ApproxLateralView %s %s AS %s\n", lateralFunc.toString(), tableAlias, columnAlias));
        return s.toString();
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        return new ArrayList<ColNameExpr>();
    }

    public String getTableAlias() { return tableAlias; }

    public String getColumnAlias() {
        return columnAlias;
    }

}
