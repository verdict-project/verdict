package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.LateralFunc;
import edu.umich.verdict.util.VerdictLogger;

/**
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+LateralView#LanguageManualLateralView-LateralViewSyntax
 * @author Yongjoo Park
 *
 */
public class LateralViewRelation extends ExactRelation {
    
    protected ExactRelation source;
    
    protected LateralFunc lateralFunc;
    
    public ExactRelation getSource() {
        return source;
    }

    public LateralFunc getLateralFunc() {
        return lateralFunc;
    }

    public LateralViewRelation(VerdictContext vc, ExactRelation source, LateralFunc lateralFunc) {
        super(vc);
        this.source = source;
        this.lateralFunc = lateralFunc;
        this.alias = source.getAlias() + "-" + lateralFunc.getTableAlias();
    }

    @Override
    protected String getSourceName() {
        VerdictLogger.error(this, "The source name of a lateral view should not be called.");
        return null;
    }

    @Override
    public ApproxRelation approx() throws VerdictException {
        ApproxRelation a = new ApproxLateralViewRelation(vc, source.approx(), lateralFunc);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        ApproxRelation a = new ApproxLateralViewRelation(vc, source.approxWith(replace), lateralFunc);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        List<ApproxRelation> ofSource = source.nBestSamples(elem, n);
        List<ApproxRelation> lviews = new ArrayList<ApproxRelation>();
        for (ApproxRelation a : ofSource) {
            lviews.add(new ApproxLateralViewRelation(vc, a, lateralFunc));
        }
        return lviews;
    }

    @Override
    public ColNameExpr partitionColumn() {
        return source.partitionColumn();
    }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        return source.accumulateSamplingProbColumns();
    }

    @Override
    protected String toStringWithIndent(String indent) {
        return null;
    }

    @Override
    public String toSql() {
        // TODO Auto-generated method stub
        return null;
    }

}
