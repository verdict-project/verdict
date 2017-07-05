package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.VerdictLogger;

public class JoinedRelation extends ExactRelation {
	
	private ExactRelation source1;
	
	private ExactRelation source2;
	
	private List<Pair<Expr, Expr>> joinCols;
	
	public JoinedRelation(VerdictContext vc, ExactRelation source1, ExactRelation source2, List<Pair<Expr, Expr>> joinCols) {
		super(vc);
		this.source1 = source1;
		this.source2 = source2;
		
		if (joinCols == null) {
			this.joinCols = new ArrayList<Pair<Expr, Expr>>();
		} else {
			this.joinCols = joinCols;
		}
		
		this.alias = null;
	}
	
	public static JoinedRelation from(VerdictContext vc, ExactRelation source1, ExactRelation source2, List<Pair<Expr, Expr>> joinCols) {
		JoinedRelation r = new JoinedRelation(vc, source1, source2, joinCols);
		return r;
	}
	
	public static JoinedRelation from(VerdictContext vc, ExactRelation source1, ExactRelation source2, Cond cond) throws VerdictException {
		return from(vc, source1, source2, extractJoinConds(cond));
	}
	
	public List<Pair<Expr, Expr>> getJoinCond() {
		return joinCols;
	}
	
	public void setJoinCond(Cond cond) throws VerdictException {
		List<Pair<Expr, Expr>> joinColumns = extractJoinConds(cond);
		this.joinCols = joinColumns;
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
	
	protected String joinClause() {
		StringBuilder sql = new StringBuilder(100);
		
		if (joinCols == null || joinCols.size() == 0) {
			VerdictLogger.info(this, "No join conditions specified; cross join is used.");
			sql.append(String.format("%s CROSS JOIN %s", sourceExpr(source1), sourceExpr(source2)));
		} else {
			sql.append(String.format("%s INNER JOIN %s ON", sourceExpr(source1), sourceExpr(source2)));
			for (int i = 0; i < joinCols.size(); i++) {
				if (i != 0) sql.append(" AND");
				sql.append(String.format(" %s = %s",
						attachTableNameIfEmpty(joinCols.get(i).getLeft(), source1.getSourceName()),
						attachTableNameIfEmpty(joinCols.get(i).getRight(), source2.getSourceName())));
			}
		}
		
		return sql.toString();
	}
	
	private String attachTableNameIfEmpty(Expr colName, String tableName) {
		ColNameExpr c = ColNameExpr.from(colName.toString());
		if (c.getTab() == null) {
			c.setTab(tableName);
		}
		return c.toString();
	}
	
	
	/*
	 * Approx
	 */
	
	public ApproxRelation approx() throws VerdictException {
		return null;
	}
	
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		ApproxRelation a = new ApproxJoinedRelation(vc, source1.approxWith(replace), source2.approxWith(replace), joinCols);
		a.setAliasName(getAliasName());
		return a;
	}

	/**
	 * Finds proper samples for each child; then, merge them.
	 */
	protected List<SampleGroup> findSample(SelectElem elem) {
		List<SampleGroup> candidates1 = source1.findSample(elem);
		List<SampleGroup> candidates2 = source2.findSample(elem);
		return combineCandidates(candidates1, candidates2);
	}
	
	private List<SampleGroup> combineCandidates(
			List<SampleGroup> candidates1,
			List<SampleGroup> candidates2) {
		List<SampleGroup> combined = new ArrayList<SampleGroup>();
		
		for (SampleGroup c1 : candidates1) {
			Set<SampleParam> set1 = c1.sampleSet();
			double cost1 = c1.cost();
			double samplingProb1 = c1.samplingProb();
			String type1 = c1.sampleType();
			
			for (SampleGroup c2 : candidates2) {
				Set<SampleParam> set2 = c2.sampleSet();
				double cost2 = c2.cost();
				double samplingProb2 = c2.samplingProb();
				String type2 = c2.sampleType();
				
				Set<SampleParam> union = new HashSet<SampleParam>(set1);
				union.addAll(set2);
				
				// add benefits to universe samples if they coincide with the join columns.
				if (universeSampleApplicable(set1, set2)) {
					combined.add(new SampleGroup(union, c1.getElems(), Math.min(samplingProb1, samplingProb2), cost1 + cost2));
				} else {
					Set<String> joinedType = ImmutableSet.of(type1, type2);
					if (joinedType.equals(ImmutableSet.of("stratified", "universe"))
						|| joinedType.equals(ImmutableSet.of("universe", "universe"))) {
						// not allowed
					} else {
						combined.add(new SampleGroup(union, c1.getElems(), samplingProb1 * samplingProb2, cost1 + cost2));
					}
				}
			}
		}
		return combined;
	}
	
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
	 * @param set A set of sample columns (for possibly multiple joined tables)
	 * @param expr A set of join columns to check
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
			Set<String> paramCols = new HashSet<String>(param.columnNames);
			if (param.sampleType.equals("universe")
					&& param.originalTable.tableName.equals(t)
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

	@Override
	public List<SelectElem> getSelectList() {
		List<SelectElem> elems = new ArrayList<SelectElem>();
		elems.addAll(source1.getSelectList());
		elems.addAll(source2.getSelectList());
		return elems;
	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col1 = source1.partitionColumn();
		ColNameExpr col2 = source2.partitionColumn();
		if (col1 != null) {
			return col1;
		} else {
			return col2;
		}
	}

	@Override
	public List<ColNameExpr> accumulateSamplingProbColumns() {
		List<ColNameExpr> union = new ArrayList<ColNameExpr>(source1.accumulateSamplingProbColumns());
		union.addAll(source2.accumulateSamplingProbColumns());
		return union;
	}
}
