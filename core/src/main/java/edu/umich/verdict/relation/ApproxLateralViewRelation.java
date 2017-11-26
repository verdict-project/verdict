package edu.umich.verdict.relation;

import java.util.List;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.LateralFunc;
import edu.umich.verdict.relation.expr.TableNameExpr;

public class ApproxLateralViewRelation extends ApproxRelation {
    
    protected ApproxRelation source;
    
    protected LateralFunc lateralFunc;
    
    public ApproxRelation getSource() {
        return source;
    }

    public LateralFunc getLateralFunc() {
        return lateralFunc;
    }

    public ApproxLateralViewRelation(VerdictContext vc, ApproxRelation source, LateralFunc lateralFunc) {
        super(vc);
        this.source = source;
        this.lateralFunc = lateralFunc;
        this.alias = source.getAlias() + "-" + lateralFunc.getTableAlias();
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        ExactRelation r = new LateralViewRelation(vc, source.rewriteForPointEstimate(), lateralFunc);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    protected ExactRelation rewriteWithPartition() {
        ExactRelation r = new LateralViewRelation(vc, source.rewriteWithPartition(), lateralFunc);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        return source.samplingProbabilityExprsFor(f);
    }

    @Override
    public double samplingProbability() {
        return source.samplingProbability();
    }

    @Override
    public double cost() {
        return source.cost();
    }

    @Override
    public Expr tupleProbabilityColumn() {
        return source.tupleProbabilityColumn();
    }

    @Override
    public Expr tableSamplingRatio() {
        return source.tableSamplingRatio();
    }

    @Override
    public String sampleType() {
        return source.sampleType();
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        return source.getColumnsOnWhichSamplesAreCreated();
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return source.tableSubstitution();
    }

    @Override
    protected boolean doesIncludeSample() {
        return source.doesIncludeSample();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxLateralViewRelation) {
            if (source.equals(((ApproxLateralViewRelation) o).getSource()) &&
                lateralFunc.equals(((ApproxLateralViewRelation) o).getLateralFunc())) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        return null;
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        return source.getAssociatedColumnNames(tabExpr);
    }

}
