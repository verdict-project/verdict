package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Represents aggregation operations on any source relation. This relation is expected to be a child of a
 * ProjectedRelation instance (which is always ensured when this instance is created from a sql statement).
 * @author Yongjoo Park
 * 
 * This class provides extended operations than ProjectedRelation
 *
 */
public class AggregatedRelation extends ExactRelation {

    protected ExactRelation source;

    protected List<SelectElem> elems;

    private boolean includeGroupsInToSql = true;

    protected AggregatedRelation(VerdictContext vc, ExactRelation source, List<SelectElem> elems) {
        super(vc);
        this.source = source;
        this.elems = elems;
        subquery = true;
    }

    @Override
    protected String getSourceName() {
        return getAlias();
    }

    public ExactRelation getSource() {
        return source;
    }

    public List<SelectElem> getElemList() {
        return elems;
    }

    public void setIncludeGroupsInToSql(boolean o) {
        includeGroupsInToSql = o;
    }

    /*
     * Approx
     */

    /**
     * if the source is a grouped relation, only a few sample types are allowed as a source of
     * another aggregate relation.
     * 1. stratified sample: the groupby column must be equal to the columns on which samples were built on.
     *    the sample type of the aggregated relation will be "nosample" and the sample type will be "1.0".
     * 2. universe sample: the groupby column must be equal to the columns on which samples where built on.
     *    the sample type of the aggregated relation will be "universe" sampled on the same columns.
     *    the sampling probability will also stays the same.
     */
    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        List<ApproxRelation> candidates = new ArrayList<ApproxRelation>();
        List<ApproxRelation> sourceCandidates = source.nBestSamples(elem, 10);
        for (ApproxRelation sc : sourceCandidates) {
            boolean eligible = false;

            if (sc.sampleType().equals("nosample")) {
                eligible = true;
            } else {
                if (sc instanceof ApproxGroupedRelation) {
                    List<Expr> groupby = ((ApproxGroupedRelation) sc).getGroupby();
                    List<String> strGroupby = new ArrayList<String>();
                    for (Expr expr : groupby) {
                        if (expr instanceof ColNameExpr) {
                            strGroupby.add(((ColNameExpr) expr).getCol());
                        }
                    }

                    String sampleType = sc.sampleType();
                    List<String> sampleColumns = sc.sampleColumns();
                    if (sampleType.equals("universe") && strGroupby.equals(sampleColumns)) {
                        eligible = true;
                    } else if (sampleType.equals("stratified") && strGroupby.equals(sampleColumns)) {
                        eligible = true;
                    }
                } else {
                    eligible = true;
                }
            }

            if (eligible) {
                ApproxRelation c = new ApproxAggregatedRelation(vc, sc, elems);
                c.setAlias(getAlias());
                candidates.add(c);
            }
        }
        return candidates;

        //		return Arrays.asList(approx());
    }

    public ApproxRelation approx() throws VerdictException {
        // these are candidates for the sources of this relation
        List<List<SampleGroup>> candidates_list = new ArrayList<List<SampleGroup>>();
        
        for (int i = 0; i < elems.size(); i++) {
            SelectElem elem = elems.get(i);
//            if (!elem.isagg()) continue;
            
            Expr agg = elem.getExpr();
            List<ApproxRelation> candidates = source.nBestSamples(agg, 10);		// TODO: make this number (10) configurable.
            List<SampleGroup> sampleGroups = new ArrayList<SampleGroup>();
            for (ApproxRelation a : candidates) {
                sampleGroups.add(new SampleGroup(a, Arrays.asList(elem)));
            }
            candidates_list.add(sampleGroups);
        }

//        // check if any of them include sample tables. If no sample table is included, we do not approximate.
//        boolean includeSample = false;
//        for (List<SampleGroup> candidates : candidates_list) {
//            for (SampleGroup g : candidates) {
//                if (g.samplingProb() < 1.0) {
//                    includeSample = true;
//                    break;
//                }
//            }
//            if (includeSample) break;
//        }
//
//        if (!includeSample) {
//            return new NoApproxRelation(this);
//        }

        // We test if we can consolidate those sample candidates so that the number of select statements is less than
        // the number of the expressions. In the worst case (e.g., all count-distinct), the number of select statements
        // will be equal to the number of the expressions. If the cost of running those select statements individually
        // is higher than the cost of running a single select statement using the original tables, samples are not used.
        SamplePlan plan = consolidate(candidates_list);
        if (plan == null) {
            String msg = "No feasible sample plan is found.";
            VerdictLogger.error(this, msg);
            throw new VerdictException(msg);
        }
        VerdictLogger.debug(this, "The sample plan to use: ");
        VerdictLogger.debugPretty(this, plan.toPrettyString(), "  ");

        //		// we create multiple aggregated relations, which, when combined, can answer the user-submitted query.
        //		List<ApproxAggregatedRelation> individuals = new ArrayList<ApproxAggregatedRelation>();
        //		for (SampleGroup group : plan.getSampleGroups()) {
        //			List<Expr> elems = group.getElems();
        //			Set<SampleParam> samplesPart = group.sampleSet();
        //			individuals.add(new ApproxAggregatedRelation(vc, source.approxWith(attachTableMapping(samplesPart)), elems));
        //		}

        List<SampleGroup> aggregateSources = plan.getSampleGroups();
        
//        List<ApproxRelation> individualSources = plan.getApproxRelations();

        // Join the results from those multiple relations (if there are more than one)
        // The root (r) will be AggregatedRelation if there is only one relation
        // The root (r) will be a ProjectedRelation of JoinedRelation if there are more than one relation
        ApproxRelation r = new ApproxAggregatedRelation(vc, aggregateSources.get(0).getSample(), aggregateSources.get(0).getElems());
        
        if (aggregateSources.size() > 1) {
            for (int i = 1; i < aggregateSources.size(); i++) {
                ApproxRelation s1 = aggregateSources.get(i).getSample();
                List<SelectElem> elems1 = aggregateSources.get(i).getElems();
                ApproxRelation r1 = new ApproxAggregatedRelation(vc, s1, elems1);
                
                String ln = r.getAlias();
                String rn = r1.getAlias();
                
                if (!(s1 instanceof ApproxGroupedRelation)) {
                    r = new ApproxJoinedRelation(vc, r, r1, null);
                } else {
                    List<Expr> groupby = ((ApproxGroupedRelation) s1).getGroupby();
                    List<Pair<Expr, Expr>> joincols = new ArrayList<Pair<Expr, Expr>>();
                    for (Expr col : groupby) {
                        // replace table names in internal colNameExpr
                        joincols.add(Pair.of(col.withTableSubstituted(ln), col.withTableSubstituted(rn)));
                    }
                    r = new ApproxJoinedRelation(vc, r, r1, joincols);
                }
            }

//            // if two or more tables are joined, groupby columns become ambiguous. So, we project out the groupby columns
//            // in the joined relations.
//            // for aggregate expressions, we use their aliases.
//            ApproxRelation firstSource = individuals.get(0).getSource();
//            if (firstSource instanceof ApproxGroupedRelation) {
//                List<Expr> groupby = ((ApproxGroupedRelation) firstSource).getGroupby();
//                List<SelectElem> newElems = new ArrayList<SelectElem>();
//                for (Expr g : groupby) {
//                    newElems.add(new SelectElem(vc, g.withTableSubstituted(individuals.get(0).getAlias())));
//                }
//                for (ApproxProjectedRelation a : allJoined) {
//                    List<SelectElem> elems = a.getSelectElems();
//                    for (SelectElem e : elems) {
//                        if (!e.isagg()) continue;
//                        newElems.add(new SelectElem(vc, ConstantExpr.from(vc, e.getAlias())));
//                    }
//                }
////                for (Expr elem : aggs) {
////                    newElems.add(new SelectElem(vc, ConstantExpr.from(vc, elem), Relation.genColumnAlias()));
////                }
//                r = new ApproxProjectedRelation(vc, r, newElems);
//            }
        }

        r.setAlias(getAlias());
        return r;
    }

    private Map<TableUniqueName, SampleParam> attachTableMapping(Set<SampleParam> samplesPart) {
        Map<TableUniqueName, SampleParam> map = new HashMap<TableUniqueName, SampleParam>();
        for (SampleParam param : samplesPart) {
            map.put(param.originalTable, param);
        }
        return map;
    }

    public ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        return new ApproxAggregatedRelation(vc, source.approxWith(replace), elems);
    }

    /*
     * Sql
     */	

    protected String selectSql() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");

        sql.append(Joiner.on(", ").join(elems));
        return sql.toString();
    }

    @Deprecated
    protected String withoutSelectSql() {
        StringBuilder sql = new StringBuilder();

        Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
        List<Expr> groupby = groupsAndNextR.getLeft();

        Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
        String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";

        sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
        if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
        if (groupby.size() > 0) { sql.append(" GROUP BY "); sql.append(Joiner.on(", ").join(groupby)); }
        return sql.toString();
    }

    public String toSql() {
        StringBuilder sql = new StringBuilder();

        Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
        List<Expr> groupby = groupsAndNextR.getLeft();

        Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
        String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";

        sql.append(selectSql());
        sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
        if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
        if (groupby.size() > 0) { sql.append(" GROUP BY "); sql.append(Joiner.on(", ").join(groupby)); }
        return sql.toString();
    }

    //	@Override
    //	public List<SelectElem> getSelectList() {
    //		List<SelectElem> elems = new ArrayList<SelectElem>();
    //		
    //		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
    //		List<Expr> groupby = groupsAndNextR.getLeft();
    //		for (Expr g : groupby) {
    //			elems.add(new SelectElem(g));
    //		}
    //		
    //		elems.addAll(this.aggs);
    //		
    //		return elems;
    //	}

    //	@Override
    //	public List<SelectElem> selectElemsWithAggregateSource() {
    //		return aggs;
    //	}

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        ColNameExpr expr = new ColNameExpr(vc, samplingProbabilityColumnName(), getAlias());
        return Arrays.asList(expr);
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(), Joiner.on(", ").join(elems)));
        s.append(source.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public ColNameExpr partitionColumn() {
        ColNameExpr col = new ColNameExpr(vc, partitionColumnName(), getAlias());
        return col;
    }

    @Override
    public Expr tupleProbabilityColumn() {
        return new ColNameExpr(vc, samplingProbabilityColumnName(), getAlias());
    }

    @Override
    public Expr tableSamplingRatio() {
        return new ColNameExpr(vc, samplingRatioColumnName(), getAlias());
    }

    @Override
    public Expr distinctCountPartitionColumn() {
        // TODO Auto-generated method stub
        return null;
    }

}
