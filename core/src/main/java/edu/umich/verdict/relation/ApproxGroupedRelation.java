package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;

public class ApproxGroupedRelation extends ApproxRelation {

    private ApproxRelation source;

    private List<Expr> groupby;

    public ApproxGroupedRelation(VerdictContext vc, ApproxRelation source, List<Expr> groupby) {
        super(vc);
        this.source = source;
        this.groupby = groupby;
        this.alias = source.alias;
    }

    public ApproxRelation getSource() {
        return source;
    }

    public List<Expr> getGroupby() {
        return groupby;
    }

    @Override
    public ExactRelation rewriteForPointEstimate() {
        List<Expr> newGroupby = groupbyWithTablesSubstituted();
        ExactRelation r = new GroupedRelation(vc, source.rewriteForPointEstimate(), newGroupby);
        r.setAlias(r.getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithPartition() {
        ExactRelation newSource = source.rewriteWithPartition();
        List<Expr> newGroupby = groupbyWithTablesSubstituted();
        //		newGroupby.add((ColNameExpr) exprWithTableNamesSubstituted(partitionColumn(), tableSubstitution()));
        newGroupby.add(newSource.partitionColumn());
        ExactRelation r = new GroupedRelation(vc, newSource, newGroupby);
        r.setAlias(r.getAlias());
        return r;
    }

    //	@Override
    //	protected ColNameExpr partitionColumn() {
    //		return source.partitionColumn();
    //	}

    @Override
    // TODO: make this more accurate for handling IN and EXISTS predicates.
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        return source.samplingProbabilityExprsFor(f);
    }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        return source.tableSubstitution();
    }

    protected List<Expr> groupbyWithTablesSubstituted() {
        Map<TableUniqueName, String> sub = tableSubstitution();
        List<Expr> replaced = new ArrayList<Expr>();
        for (Expr e : groupby) {
            replaced.add(exprWithTableNamesSubstituted(e, sub));
        }
        return replaced;
    }
    
    private Set<String> groupbyInString() {
        List<Expr> groupby = getGroupby();
        Set<String> strGroupby = new HashSet<String>();
        for (Expr expr : groupby) {
            if (expr instanceof ColNameExpr) {
                strGroupby.add(((ColNameExpr) expr).getCol());
            }
        }
        return strGroupby;
    }

    @Override
    public String sampleType() {
        String sampleType = source.sampleType();
        if (sampleType.equals("nosample")) return "nosample";
        
        Set<String> groupbyStr = groupbyInString();
        Set<String> sampleColumns = new HashSet<String>(source.sampleColumns());
        
        if (sampleType.equals("universe") && groupbyStr.equals(sampleColumns)) {
            return "universe";
        } else if (sampleType.equals("stratified") && groupbyStr.equals(sampleColumns)) {
            return "nosample";
        }

        return "grouped-" + sampleType;
    }

    @Override
    public double cost() {
        return source.cost();
    }

    @Override
    protected List<String> sampleColumns() {
        return source.sampleColumns();
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s, %s (%s)) [%s]\n",
                this.getClass().getSimpleName(),
                getAlias(),
                sampleType(),
                sampleColumns().toString(),
                Joiner.on(", ").join(groupby)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxGroupedRelation) {
            if (source.equals(((ApproxGroupedRelation) o).source)) {
                if (groupby.equals(((ApproxGroupedRelation) o).groupby)) {
                    return true;
                }
            }
        }
        return false;
    }

    // assumes that this method is called by the parent, i.e., ApproxAggregatedRelation.
    @Override
    public double samplingProbability() {
        Set<String> groupbyStr = groupbyInString();
        Set<String> sampleColumns = new HashSet<String>(source.sampleColumns());
        
        if (sampleColumns.equals(groupbyStr)) {
            if (sampleType().equals("universe")) {
                return Math.min(2 * source.samplingProbability(), 1.0);
            } else if (sampleType().equals("nosample")) {
                return Math.min(5 * source.samplingProbability(), 1.0);
            }
        }
        
        return source.samplingProbability();
    }

    @Override
    protected boolean doesIncludeSample() {
        return source.doesIncludeSample();
    }
    
    @Override
    public Expr tupleProbabilityColumn() {
        return source.tupleProbabilityColumn();
    }

    @Override
    public Expr tableSamplingRatio() {
        return source.tableSamplingRatio();
    }

}
