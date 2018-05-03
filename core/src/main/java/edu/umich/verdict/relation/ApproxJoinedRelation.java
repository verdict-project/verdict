/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umich.verdict.util.VerdictLogger;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.JoinedRelation.JoinType;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.TableNameExpr;

public class ApproxJoinedRelation extends ApproxRelation {

    private ApproxRelation source1;

    private ApproxRelation source2;

    private List<Pair<Expr, Expr>> joinCols;
    
    private JoinType joinType = JoinType.INNER;

    public JoinType getJoinType() {
		return joinType;
	}

	public void setJoinType(JoinType joinType) {
		this.joinType = joinType;
	}

	/**
     * 
     * @param vc
     * @param source1
     * @param source2
     * @param joinCols
     *            An empty joinCols indicates CROSS JOIN
     */
    public ApproxJoinedRelation(VerdictContext vc, ApproxRelation source1, ApproxRelation source2,
            List<Pair<Expr, Expr>> joinCols) {
        super(vc);
        this.source1 = source1;
        this.source2 = source2;
        if (joinCols == null) {
            this.joinCols = new ArrayList<Pair<Expr, Expr>>();
        } else {
            this.joinCols = joinCols;
        }
        this.alias = String.format("%s_%s", source1.getAlias(), source2.getAlias());
    }

    public ApproxJoinedRelation(VerdictContext vc, ApproxRelation source1, ApproxRelation source2) {
        this(vc, source1, source2, Arrays.<Pair<Expr, Expr>>asList());
    }

    public static ApproxJoinedRelation from(VerdictJDBCContext vc, ApproxRelation source1, ApproxRelation source2,
            List<Pair<Expr, Expr>> joinCols) {
        ApproxJoinedRelation r = new ApproxJoinedRelation(vc, source1, source2, joinCols);
        return r;
    }

    public static ApproxJoinedRelation from(VerdictJDBCContext vc, ApproxRelation source1, ApproxRelation source2,
            Cond cond) throws VerdictException {
        return from(vc, source1, source2, extractJoinConds(cond));
    }

    private static List<Pair<Expr, Expr>> extractJoinConds(Cond cond) throws VerdictException {
        if (cond == null) {
            return null;
        }
        if (cond instanceof CompCond) {
            CompCond cmp = (CompCond) cond;
            List<Pair<Expr, Expr>> l = new ArrayList<Pair<Expr, Expr>>();
            l.add(Pair.of(cmp.getLeft(), cmp.getRight()));
            return l;
        } else if (cond instanceof AndCond) {
            AndCond and = (AndCond) cond;
            List<Pair<Expr, Expr>> l = new ArrayList<Pair<Expr, Expr>>();
            l.addAll(extractJoinConds(and.getLeft()));
            l.addAll(extractJoinConds(and.getRight()));
            return l;
        } else {
            throw new VerdictException("Join condition must be an 'and' condition.");
        }
    }

    /*
     * Approx
     */

    @Override
    public ExactRelation rewriteForPointEstimate() {
        List<Pair<Expr, Expr>> newJoinCond = joinCondWithTablesSubstitutioned();
        ExactRelation r = new JoinedRelation(vc, source1.rewriteForPointEstimate(), source2.rewriteForPointEstimate(),
                newJoinCond);
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithSubsampledErrorBounds() {
        ExactRelation r1 = source1.rewriteWithSubsampledErrorBounds();
        ExactRelation r2 = source2.rewriteWithSubsampledErrorBounds();
        List<Pair<Expr, Expr>> newJoinCond = joinCondWithTablesSubstitutioned();
        return new JoinedRelation(vc, r1, r2, newJoinCond);
    }

    @Override
    public ExactRelation rewriteWithPartition() {
        ExactRelation newSource1 = source1.rewriteWithPartition();
        ExactRelation newSource2 = source2.rewriteWithPartition();

        List<Pair<Expr, Expr>> newJoinCond = joinCondWithTablesSubstitutioned();
        // newJoinCond.add(Pair.<Expr, Expr>of(newSource1.partitionColumn(),
        // newSource2.partitionColumn()));
        JoinedRelation r = JoinedRelation.from(vc, newSource1, newSource2, newJoinCond);
        r.setJoinType(getJoinType());
        r.setAlias(getAlias());
        return r;
    }

    protected List<Pair<Expr, Expr>> joinCondWithTablesSubstitutioned() {
        Map<TableUniqueName, String> sub = tableSubstitution();
        // replaces the table names in the join conditions with the sample tables.
        List<Pair<Expr, Expr>> cols = new ArrayList<Pair<Expr, Expr>>();
        for (Pair<Expr, Expr> p : joinCols) {
            cols.add(Pair.of(exprWithTableNamesSubstituted(p.getLeft(), sub),
                    exprWithTableNamesSubstituted(p.getRight(), sub)));
        }
        return cols;
    }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        if (Relation.areMatchingUniverseSamples(source1, source2, joinCols)) {
            // get the first pair to check the table names to be joined.
            Pair<Expr, Expr> ajoinCol = joinCols.get(0);
            Expr l = ajoinCol.getLeft();
            Expr r = ajoinCol.getRight();

            // we properly handles a join of two universe samples only if the join
            // conditions are column names.
            // that is, they should not be some expressions of those column names.
            if ((l instanceof ColNameExpr) && (r instanceof ColNameExpr)) {
                List<Expr> samplingProbExprs = new ArrayList<Expr>();
                ColNameExpr rc = (ColNameExpr) r;
                // add all sampling probability columns from the left table.
                // here, we make an assumption that the sampling probabilities of the universe
                // samples to be joined
                // are equal.
                samplingProbExprs.addAll(source1.samplingProbabilityExprsFor(f));

                // when adding right expressions from the right table, we exclude the column on
                // which universe sample
                // is created.
                for (Expr e : source2.samplingProbabilityExprsFor(f)) {
                    if ((e instanceof ColNameExpr) && ((ColNameExpr) e).getTab().equals(rc.getTab())) {
                        continue;
                    } else {
                        samplingProbExprs.add(e);
                    }
                }
                return samplingProbExprs;
            }
        }

        List<Expr> samplingProbExprs = new ArrayList<Expr>(source1.samplingProbabilityExprsFor(f));
        samplingProbExprs.addAll(source2.samplingProbabilityExprsFor(f));
        return samplingProbExprs;
    }

    // private boolean areMatchingUniverseSamples() {
    // List<Expr> leftJoinCols = new ArrayList<Expr>();
    // List<Expr> rightJoinCols = new ArrayList<Expr>();
    // for (Pair<Expr, Expr> pair : joinCols) {
    // leftJoinCols.add(pair.getLeft());
    // rightJoinCols.add(pair.getRight());
    // }
    //
    // return source1.sampleType().equals("universe") &&
    // source2.sampleType().equals("universe")
    // && joinColumnsEqualToSampleColumns(leftJoinCols, source1.sampleColumns())
    // && joinColumnsEqualToSampleColumns(rightJoinCols, source2.sampleColumns());
    // }

    @Override
    public String sampleType() {
        Set<String> sampleTypeSet = ImmutableSet.of(source1.sampleType(), source2.sampleType());

        if (Relation.areMatchingUniverseSamples(source1, source2, joinCols)) {
            return "universe";
        } else if (sampleTypeSet.equals(ImmutableSet.of("uniform", "uniform"))) {
            return "uniform";
        } else if (sampleTypeSet.equals(ImmutableSet.of("uniform", "stratified"))) {
            return "stratified";
        } else if (sampleTypeSet.equals(ImmutableSet.of("uniform", "universe"))) {
            return "uniform";
        } else if (sampleTypeSet.equals(ImmutableSet.of("uniform", "nosample"))) {
            return "uniform";
        } else if (sampleTypeSet.equals(ImmutableSet.of("stratified", "stratified"))) {
            return "arbitrary";
        } else if (sampleTypeSet.equals(ImmutableSet.of("stratified", "nosample"))) {
            return "stratified";
        } else if (sampleTypeSet.equals(ImmutableSet.of("universe", "nosample"))) {
            return "universe";
        } else if (sampleTypeSet.equals(ImmutableSet.of("nosample", "nosample"))) {
            return "nosample";
        } else {
            return source1.sampleType() + "-" + source2.sampleType(); // unexpected
        }
    }

    @Override
    public double cost() {
        return source1.cost() + source2.cost();
    }

    @Override
    public List<String> getColumnsOnWhichSamplesAreCreated() {
        if (sampleType().equals("stratified")) {
            List<String> union = new ArrayList<String>(source1.getColumnsOnWhichSamplesAreCreated());
            union.addAll(source2.getColumnsOnWhichSamplesAreCreated());
            return union;
        } else if (sampleType().equals("universe")) {
            if (source1.sampleType().equals("universe")) {
                return source1.getColumnsOnWhichSamplesAreCreated();
            } else {
                return source2.getColumnsOnWhichSamplesAreCreated();
            }
        } else {
            return Arrays.asList();
        }
    }

    // private boolean joinColumnsEqualToSampleColumns(List<Expr> joinCols,
    // List<String> sampleColNames) {
    // List<String> joinColNames = new ArrayList<String>();
    // for (Expr expr : joinCols) {
    // if (expr instanceof ColNameExpr) {
    // joinColNames.add(((ColNameExpr) expr).getCol());
    // }
    // }
    // return joinColNames.equals(sampleColNames);
    // }

    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        Map<TableUniqueName, String> sub1 = source1.tableSubstitution();
        Map<TableUniqueName, String> sub2 = source2.tableSubstitution();
        
        HashMap<TableUniqueName, String> m = new HashMap<TableUniqueName, String>();
        m.putAll(sub1);
        m.putAll(sub2);
        return m;
//        return ImmutableMap.<TableUniqueName, String>builder().putAll(sub1).putAll(sub2).build();
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s], sample type: %s (%s), sampling prob: %f, cost: %f\n",
                this.getClass().getSimpleName(), getAlias(), Joiner.on(", ").join(joinCols), sampleType(),
                getColumnsOnWhichSamplesAreCreated(), samplingProbability(), cost()));
        s.append(source1.toStringWithIndent(indent + "  "));
        s.append(source2.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    @Override
    public boolean equals(ApproxRelation o) {
        if (o instanceof ApproxJoinedRelation) {
            if (source1.equals(((ApproxJoinedRelation) o).source1)
                    && source2.equals(((ApproxJoinedRelation) o).source2)) {
                if (joinCols.equals(((ApproxJoinedRelation) o).joinCols)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public double samplingProbability() {
        if (Relation.areMatchingUniverseSamples(source1, source2, joinCols)) {
            return source1.samplingProbability();
        } else {
            return source1.samplingProbability() * source2.samplingProbability();
        }
    }

    @Override
    protected boolean doesIncludeSample() {
        return source1.doesIncludeSample() || source2.doesIncludeSample();
    }

    @Override
    public Expr tupleProbabilityColumn() {
        Expr expr1 = source1.tupleProbabilityColumn();
        Expr expr2 = source2.tupleProbabilityColumn();

        if (sampleType().equals("universe")) {
            return expr1;
        } else {
            Expr combined = new BinaryOpExpr(vc, expr1, expr2, "*");
            return combined;
        }
    }

    @Override
    public Expr tableSamplingRatio() {
        Expr expr1 = source1.tableSamplingRatio();
        Expr expr2 = source2.tableSamplingRatio();
        Expr combined = new BinaryOpExpr(vc, expr1, expr2, "*");
        return combined;
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        List<ColNameExpr> colnames = new ArrayList<ColNameExpr>();
        colnames.addAll(source1.getAssociatedColumnNames(tabExpr));
        colnames.addAll(source2.getAssociatedColumnNames(tabExpr));
        return colnames;
    }

    //direct copy from JoinedRelation
    protected static Map<JoinType, String> joinTypeString =
            ImmutableMap.<JoinType, String>builder()
                    .put(JoinType.INNER, "INNER JOIN")
                    .put(JoinType.CROSS, "CROSS JOIN")
                    .put(JoinType.LATERAL, "LATERAL VIEW")
                    .put(JoinType.LEFT_OUTER, "LEFT OUTER JOIN")
                    .put(JoinType.RIGHT_OUTER, "RIGHT OUTER JOIN")
                    .put(JoinType.LEFT_SEMI, "LEFT SEMI JOIN")
                    .build();

    protected String joinClause() {
        StringBuilder sql = new StringBuilder(100);

        if (joinType.equals(JoinType.CROSS) || joinType.equals(JoinType.LATERAL)) {
            sql.append(String.format("%s %s %s", sourceExpr(source1), joinTypeString.get(joinType), sourceExpr(source2)));
        }
        else if (joinCols == null || joinCols.size() == 0) {
            VerdictLogger.debug(this, "No join conditions specified; cross join is used.");
            sql.append(String.format("%s CROSS JOIN %s", sourceExpr(source1), sourceExpr(source2)));
        }
        else {      // INNER JOIN, LEFT OUTER, RIGHT OUTER
            sql.append(String.format("%s %s %s ON", sourceExpr(source1), joinTypeString.get(joinType), sourceExpr(source2)));
            for (int i = 0; i < joinCols.size(); i++) {
                if (i != 0)
                    sql.append(" AND");
                sql.append(String.format(" %s = %s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                // attachTableNameIfEmpty(joinCols.get(i).getLeft(), source1.getSourceName()),
                // attachTableNameIfEmpty(joinCols.get(i).getRight(),
                // source2.getSourceName())));
            }
        }

        return sql.toString();
    }
}
