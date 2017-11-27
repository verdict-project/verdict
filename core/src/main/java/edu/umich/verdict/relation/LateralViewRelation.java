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
    
//    protected ExactRelation source;
    
    protected LateralFunc lateralFunc;
    
    protected String tableAlias;
    
    protected String columnAlias;
    
//    public ExactRelation getSource() {
//        return source;
//    }

    public String getTableAlias() {
        return tableAlias;
    }
    
    private void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias.toLowerCase();
    }

    public String getColumnAlias() {
        return columnAlias;
    }
    
    private void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias.toLowerCase();
    }

    public LateralFunc getLateralFunc() {
        return lateralFunc;
    }

    public LateralViewRelation(VerdictContext vc, LateralFunc lateralFunc, String tableAlias, String columnAlias) {
        super(vc);
//        this.source = source;
        this.lateralFunc = lateralFunc;
//        this.alias = source.getAlias() + "-" + lateralFunc.getTableAlias();
        setTableAlias((tableAlias == null)? genTableAlias() : tableAlias.toLowerCase());
        setColumnAlias((columnAlias == null)? genColumnAlias() : columnAlias.toLowerCase());
        setAlias(tableAlias);
    }

    @Override
    protected String getSourceName() {
        VerdictLogger.error(this, "The source name of a lateral view should not be called.");
        return null;
    }

    @Override
    public ApproxRelation approx() throws VerdictException {
        ApproxRelation a = new ApproxLateralViewRelation(vc, lateralFunc, tableAlias, columnAlias);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        ApproxRelation a = new ApproxLateralViewRelation(vc, lateralFunc, tableAlias, columnAlias);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        List<ApproxRelation> lviews = new ArrayList<ApproxRelation>();
        lviews.add(approx());
        return lviews;
    }

    @Override
    public ColNameExpr partitionColumn() {
        return null;
    }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        return new ArrayList<ColNameExpr>();
    }

    @Override
    protected String toStringWithIndent(String indent) {
    	StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("LateralView %s %s AS %s\n", lateralFunc.toString(), tableAlias, columnAlias));
        return s.toString();
    }

    @Override
    public String toSql() {
        // TODO Auto-generated method stub
        return null;
    }

}
