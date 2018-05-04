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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.umich.verdict.relation.expr.SelectElem;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.LateralFunc.LateralFuncName;
import edu.umich.verdict.util.VerdictLogger;

public class JoinedRelation extends ExactRelation {

    public enum JoinType {
	    INNER, CROSS, LATERAL, LEFT_OUTER, RIGHT_OUTER, LEFT_SEMI
	}

	protected static Map<JoinType, String> joinTypeString =
	ImmutableMap.<JoinType, String>builder()
	.put(JoinType.INNER, "INNER JOIN")
	.put(JoinType.CROSS, "CROSS JOIN")
	.put(JoinType.LATERAL, "LATERAL VIEW")
	.put(JoinType.LEFT_OUTER, "LEFT OUTER JOIN")
	.put(JoinType.RIGHT_OUTER, "RIGHT OUTER JOIN")
    .put(JoinType.LEFT_SEMI, "LEFT SEMI JOIN")
	.build();

	private ExactRelation source1;

    private ExactRelation source2;

    private List<Pair<Expr, Expr>> joinCols;

	private JoinType joinType = JoinType.INNER;

	public ExactRelation getLeftSource() {
        return source1;
    }

    public ExactRelation getRightSource() {
        return source2;
    }

    public JoinType getJoinType() {
        return joinType;
    }
    
    public void setJoinType(JoinType type) {
        joinType = type;
    }
    
    public List<Pair<Expr, Expr>> getJoinCond() {
        return joinCols;
    }

    /**
     * Sets the join condition. If there are extra substitution request (by the
     * param 'subs'), we perform table name substitutions as well.
     * 
     * @param cond
     * @param subs
     * @throws VerdictException
     */
    public void setJoinCond(Cond cond, Map<TableUniqueName, String> subs) throws VerdictException {
        List<Pair<Expr, Expr>> joinColumns = extractJoinConds(cond);
    
        // if the original table names are used for join conditions, they must be
        // replaced with
        // their alias names. note that all tables are aliased internally.
        if (subs != null) {
            TableNameReplacerInExpr rep = new TableNameReplacerInExpr(vc, subs);
            List<Pair<Expr, Expr>> substituedJoinColumns = new ArrayList<Pair<Expr, Expr>>();
            for (Pair<Expr, Expr> exprs : joinColumns) {
                Expr l = rep.visit(exprs.getLeft());
                Expr r = rep.visit(exprs.getRight());
                substituedJoinColumns.add(Pair.of(l, r));
            }
            joinColumns = substituedJoinColumns;
        }
    
        this.joinCols = joinColumns;
    }

    public void setJoinCond(Cond cond) throws VerdictException {
        setJoinCond(cond, null);
    }

    public JoinedRelation(VerdictContext vc, ExactRelation source1, ExactRelation source2,
            List<Pair<Expr, Expr>> joinCols) {
        super(vc);
        this.source1 = source1;
        this.source2 = source2;

        if (joinCols == null) {
            this.joinCols = new ArrayList<Pair<Expr, Expr>>();
        } else {
            this.joinCols = joinCols;
        }

        this.alias = String.format("%s-%s", source1.getAlias(), source2.getAlias());
    }

    public static JoinedRelation from(VerdictContext vc, ExactRelation source1, ExactRelation source2,
            List<Pair<Expr, Expr>> joinCols) {
        JoinedRelation r = new JoinedRelation(vc, source1, source2, joinCols);
        return r;
    }

    public static JoinedRelation from(VerdictContext vc, ExactRelation source1, ExactRelation source2, Cond cond) {
        return from(vc, source1, source2, extractJoinConds(cond));
    }

    

    private static List<Pair<Expr, Expr>> extractJoinConds(Cond cond) {
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
            VerdictLogger.error("Join condition must be a CompCond instance possibly in an AndCond instance.");
            return null;
        }
    }

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
            if (!(source1 instanceof JoinedRelation) && !(source2 instanceof JoinedRelation)) {
                sql.append(String.format("%s %s %s ON",
                        sourceExpr(source1), joinTypeString.get(joinType), sourceExpr(source2)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" %s = %s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                    // attachTableNameIfEmpty(joinCols.get(i).getLeft(), source1.getSourceName()),
                    // attachTableNameIfEmpty(joinCols.get(i).getRight(),
                    // source2.getSourceName())));
                }
            } else if (source1 instanceof JoinedRelation && !(source2 instanceof JoinedRelation)) {
                sql.append(String.format("%s %s %s ON",
                        sourceExpr(source1), joinTypeString.get(joinType), sourceExpr(source2)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" %s = %s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                }
            } else if (source2 instanceof JoinedRelation && !(source1 instanceof JoinedRelation)) {
                sql.append(String.format("%s %s %s ON",
                        sourceExpr(source2), joinTypeString.get(joinType), sourceExpr(source1)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" %s = %s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                }
            } else if ((source1 instanceof JoinedRelation) && (source2 instanceof JoinedRelation)) {
                sql.append(String.format("(%s) %s (%s) ON",
                        sourceExpr(source1), joinTypeString.get(joinType),
                        sourceExpr(source2)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" %s = %s", joinCols.get(i).getLeft(),
                            joinCols.get(i).getRight()));
                }
            }
        }

        return sql.toString();
    }

    protected String joinClauseWithTempAlias() {
        StringBuilder sql = new StringBuilder(100);

        if (joinType.equals(JoinType.CROSS) || joinType.equals(JoinType.LATERAL)) {
            sql.append(String.format("%s %s %s", sourceExprWithTempAlias(source1), joinTypeString.get(joinType), sourceExprWithTempAlias(source2)));
        }
        else if (joinCols == null || joinCols.size() == 0) {
            VerdictLogger.debug(this, "No join conditions specified; cross join is used.");
            sql.append(String.format("%s CROSS JOIN %s", sourceExprWithTempAlias(source1), sourceExprWithTempAlias(source2)));
        }
        else {      // INNER JOIN, LEFT OUTER, RIGHT OUTER
            if (!(source1 instanceof JoinedRelation) && !(source2 instanceof JoinedRelation)) {
                sql.append(String.format("%s %s %s ON",
                        sourceExprWithTempAlias(source1), joinTypeString.get(joinType), sourceExprWithTempAlias(source2)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" tmp_%s = tmp_%s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                    // attachTableNameIfEmpty(joinCols.get(i).getLeft(), source1.getSourceName()),
                    // attachTableNameIfEmpty(joinCols.get(i).getRight(),
                    // source2.getSourceName())));
                }
            } else if (source1 instanceof JoinedRelation && !(source2 instanceof JoinedRelation)) {
                sql.append(String.format("%s %s %s ON",
                        sourceExprWithTempAlias(source1), joinTypeString.get(joinType), sourceExprWithTempAlias(source2)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" tmp_%s = tmp_%s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                }
            } else if (source2 instanceof JoinedRelation && !(source1 instanceof JoinedRelation)) {
                sql.append(String.format("%s %s %s ON",
                        sourceExprWithTempAlias(source2), joinTypeString.get(joinType), sourceExprWithTempAlias(source1)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" tmp_%s = tmp_%s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
                }
            } else if ((source1 instanceof JoinedRelation) && (source2 instanceof JoinedRelation)) {
                sql.append(String.format("(%s) %s (%s) ON",
                        sourceExprWithTempAlias(source1), joinTypeString.get(joinType),
                        sourceExprWithTempAlias(source2)));
                for (int i = 0; i < joinCols.size(); i++) {
                    if (i != 0)
                        sql.append(" AND");
                    sql.append(String.format(" tmp_%s = tmp_%s", joinCols.get(i).getLeft(),
                            joinCols.get(i).getRight()));
                }
            }
        }

        return sql.toString();
    }

    private ColNameExpr attachTableNameIfEmpty(Expr colName, String tableName) {
        ColNameExpr c = ColNameExpr.from(vc, colName.toString());
        if (c.getTab() == null) {
            c.setTab(tableName);
        }
        return c;
    }

    public boolean containsRelation(ExactRelation r, String tab) {
        if (source1 instanceof JoinedRelation) {
            JoinedRelation jr = (JoinedRelation) source1;
            boolean isContain = jr.containsRelation(r, tab);
            if (isContain) return true;
        } else {
            if (source1.getAlias().equals(tab)) {
                return true;
            }
        }
        if (source2 instanceof JoinedRelation) {
            JoinedRelation jr = (JoinedRelation) source2;
            boolean isContain = jr.containsRelation(r, tab);
            if (isContain) return true;
        } else {
            if (source2.getAlias().equals(tab)) {
                return true;
            }
        }
//        if (source1 instanceof SingleRelation || source1 instanceof AggregatedRelation ||
//                source1 instanceof ProjectedRelation) {
//            if (source1.getAlias().equals(tab)) {
//                return true;
//            }
//        } else if (source1 instanceof JoinedRelation) {
//            JoinedRelation jr = (JoinedRelation) source1;
//            boolean isContain = jr.containsRelation(r, tab);
//            if (isContain) return true;
//        }
//        if (source2 instanceof SingleRelation || source2 instanceof AggregatedRelation ||
//                source2 instanceof ProjectedRelation) {
//            if (source2.getAlias().equals(tab)) {
//                return true;
//            }
//        } else if (source2 instanceof JoinedRelation) {
//            JoinedRelation jr = (JoinedRelation) source2;
//            boolean isContain = jr.containsRelation(r, tab);
//            if (isContain) return true;
//        }
        return false;
    }

    /*
     * Approx
     */

    public ApproxRelation approx() throws VerdictException {
        ApproxRelation a = new ApproxJoinedRelation(vc, source1.approx(), source2.approx(), joinCols);
        a.setAlias(getAlias());
        return a;
    }

    public ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
        ApproxRelation a = new ApproxJoinedRelation(vc, source1.approxWith(replace),
                source2.approxWith(replace), joinCols);
        a.setAlias(getAlias());
        return a;
    }

    @Override
    protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
        List<ApproxRelation> ofSources1 = source1.nBestSamples(elem, n);
        List<ApproxRelation> ofSources2 = source2.nBestSamples(elem, n);
        List<ApproxRelation> joined = new ArrayList<ApproxRelation>();

        for (ApproxRelation a1 : ofSources1) {
            for (ApproxRelation a2 : ofSources2) {
                ApproxJoinedRelation j = new ApproxJoinedRelation(vc, a1, a2, joinCols);
                j.setJoinType(getJoinType());
                if (expectedSampleType(j.sampleType())) {
                    joined.add(j);
                }
            }
        }

        return joined;
    }

    private boolean expectedSampleType(String sampleType) {
        return availableJoinTypes.contains(sampleType);
    }

    // /**
    // * Finds proper samples for each child; then, merge them.
    // */
    // protected List<SampleGroup> findSample(Expr elem) {
    // List<SampleGroup> candidates1 = source1.findSample(elem);
    // List<SampleGroup> candidates2 = source2.findSample(elem);
    // return combineCandidates(candidates1, candidates2);
    // }
    //
    // private List<SampleGroup> combineCandidates(
    // List<SampleGroup> candidates1,
    // List<SampleGroup> candidates2) {
    // List<SampleGroup> combined = new ArrayList<SampleGroup>();
    //
    // for (SampleGroup c1 : candidates1) {
    // Set<SampleParam> set1 = c1.sampleSet();
    // double cost1 = c1.cost();
    // double samplingProb1 = c1.samplingProb();
    // String type1 = c1.sampleType();
    //
    // for (SampleGroup c2 : candidates2) {
    // Set<SampleParam> set2 = c2.sampleSet();
    // double cost2 = c2.cost();
    // double samplingProb2 = c2.samplingProb();
    // String type2 = c2.sampleType();
    //
    // Set<SampleParam> union = new HashSet<SampleParam>(set1);
    // union.addAll(set2);
    //
    // // add benefits to universe samples if they coincide with the join columns.
    // if (universeSampleApplicable(set1, set2)) {
    // combined.add(new SampleGroup(union, c1.getElems(), Math.min(samplingProb1,
    // samplingProb2), cost1 + cost2));
    // } else {
    // Set<String> joinedType = ImmutableSet.of(type1, type2);
    // if (joinedType.equals(ImmutableSet.of("stratified", "universe"))
    // || joinedType.equals(ImmutableSet.of("universe", "universe"))) {
    // // not allowed
    // } else {
    // combined.add(new SampleGroup(union, c1.getElems(), samplingProb1 *
    // samplingProb2, cost1 + cost2));
    // }
    // }
    // }
    // }
    // return combined;
    // }

    private boolean universeSampleApplicable(Set<SampleParam> set1, Set<SampleParam> set2) {
        Set<ColNameExpr> lJoinCols = new HashSet<ColNameExpr>();
        Set<ColNameExpr> rJoinCols = new HashSet<ColNameExpr>();
        for (Pair<Expr, Expr> p : joinCols) {
            if (p.getLeft() instanceof ColNameExpr) {
                lJoinCols.add((ColNameExpr) p.getLeft());
            } else {
                return false;
            }
            if (p.getRight() instanceof ColNameExpr) {
                rJoinCols.add((ColNameExpr) p.getRight());
            } else {
                return false;
            }
        }

        if (universeSampleIn(set1, lJoinCols) && universeSampleIn(set2, rJoinCols)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Checks if there exists a universe sample with the given column expression.
     * 
     * @param set
     *            A set of sample columns (for possibly multiple joined tables)
     * @param aJoinCols
     *            A set of join columns to check
     * @return
     */
    private boolean universeSampleIn(Set<SampleParam> set, Set<ColNameExpr> aJoinCols) {
        Set<String> jc = new HashSet<String>();
        String t = null;
        for (ColNameExpr c : aJoinCols) {
            jc.add(c.getCol());
            t = c.getTab();
        }
        for (SampleParam param : set) {
            Set<String> paramCols = new HashSet<String>(param.getColumnNames());
            if (param.getSampleType().equals("universe") && param.getOriginalTable().getTableName().equals(t)
                    && paramCols.equals(jc)) {
                return true;
            }
        }
        return false;
    }

    /*
     * Filtering functions
     */

    public ExactRelation filter(Cond cond) throws VerdictException {
        if (getJoinCond() == null) {
            setJoinCond(cond);
            return this;
        } else {
            return new FilteredRelation(vc, this, cond);
        }
    }

    /*
     * Sql
     */

    public String toSql() {
        return this.select("*").toSql();
    }

    @Override
    protected String getSourceName() {
        VerdictLogger.error(this, "The source name of a joined table should not be called.");
        return null;
    }

    // @Override
    // public List<SelectElem> getSelectList() {
    // List<SelectElem> elems = new ArrayList<SelectElem>();
    // elems.addAll(source1.getSelectList());
    // elems.addAll(source2.getSelectList());
    // return elems;
    // }

    @Override
    public List<ColNameExpr> accumulateSamplingProbColumns() {
        List<ColNameExpr> union = new ArrayList<ColNameExpr>(source1.accumulateSamplingProbColumns());
        union.addAll(source2.accumulateSamplingProbColumns());
        return union;
    }

    @Override
    protected String toStringWithIndent(String indent) {
        StringBuilder s = new StringBuilder(1000);
        s.append(indent);
        s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(),
                Joiner.on(", ").join(joinCols)));
        s.append(source1.toStringWithIndent(indent + "  "));
        s.append(source2.toStringWithIndent(indent + "  "));
        return s.toString();
    }

    // @Override
    // public List<SelectElem> getSelectList() {
    // List<SelectElem> elems = new ArrayList<SelectElem>();
    // elems.addAll(source1.getSelectList());
    // elems.addAll(source2.getSelectList());
    // return elems;
    // }

    @Override
    public ColNameExpr partitionColumn() {
        ColNameExpr col1 = source1.partitionColumn();
        if (col1 != null) {
            return col1;
        } else {
            ColNameExpr col2 = source2.partitionColumn();
            return col2;
        }
    }

    @Override
    public List<SelectElem> getSelectElemList() {
        List<SelectElem> selectList = source1.getSelectElemList();
        selectList.addAll(source2.getSelectElemList());
        return selectList;
    }

    // @Override
    // public Expr distinctCountPartitionColumn() {
    // Expr col1 = source1.distinctCountPartitionColumn();
    // if (col1 != null) {
    // return col1;
    // } else {
    // Expr col2 = source2.distinctCountPartitionColumn();
    // return col2;
    // }
    // }
}
