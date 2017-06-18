package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.VerdictLogger;

public class JoinedRelation extends ExactRelation {
	
	private ExactRelation source1;
	
	private ExactRelation source2;
	
	private List<Pair<Expr, Expr>> joinCols;
	
	public JoinedRelation(VerdictContext vc, ExactRelation source1, ExactRelation source2, List<Pair<Expr, Expr>> joinCols) {
		super(vc);
		this.source1 = source1;
		this.source2 = source2;
		this.joinCols = joinCols;
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
	
	protected String sourceExpr(ExactRelation source) {
		if (source instanceof SingleRelation) {
			return ((SingleRelation) source).getTableName().toString();
		} else {
			return source.toSql();
		}
	}
	
	protected String joinClause() {
		if (joinCols == null) {
			VerdictLogger.warn(this, "Join conditions are null. Inner join will throw an error.");
		}
		
		StringBuilder sql = new StringBuilder(100);
		sql.append(String.format("%s INNER JOIN %s ON", sourceExpr(source1), sourceExpr(source2)));
		for (int i = 0; i < joinCols.size(); i++) {
			if (i != 0) sql.append(" AND");
			sql.append(String.format(" %s = %s", joinCols.get(i).getLeft(), joinCols.get(i).getRight()));
		}
		return sql.toString();
	}
	
	/*
	 * Approx
	 */
	
	public ApproxRelation approx() throws VerdictException {
		return null;
	}
	
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return new ApproxJoinedRelation(vc, source1.approxWith(replace), source2.approxWith(replace), joinCols);
	}

	/**
	 * Finds proper samples for each child; then, merge them.
	 */
	protected Map<Set<SampleParam>, Double> findSample(List<Expr> aggExprs) {
		Map<Set<SampleParam>, Double> candidates1 = source1.findSample(aggExprs);
		Map<Set<SampleParam>, Double> candidates2 = source2.findSample(aggExprs);
		return combineCandidates(candidates1, candidates2);
	}
	
	private Map<Set<SampleParam>, Double> combineCandidates(
			Map<Set<SampleParam>, Double> candidates1,
			Map<Set<SampleParam>, Double> candidates2) {
		Map<Set<SampleParam>, Double> combined = new HashMap<Set<SampleParam>, Double>();
		for (Map.Entry<Set<SampleParam>,Double> c1 : candidates1.entrySet()) {
			Set<SampleParam> set1 = c1.getKey();
			for (Map.Entry<Set<SampleParam>, Double> c2 : candidates2.entrySet()) {
				Set<SampleParam> set2 = c2.getKey();
				Set<SampleParam> union = new HashSet<SampleParam>(set1);
				union.addAll(set2);
				
				// add benefits to universe samples if they coincide with the join columns.
				if (universeSampleApplicable(set1, set2)) {
					combined.put(union, c1.getValue() + c2.getValue() + 20);
				} else {
					combined.put(union, c1.getValue() + c2.getValue());
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
	
	protected String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append(String.format("SELECT * FROM %s", joinClause()));
		return sql.toString();
	}

	@Override
	protected String getSourceName() {
		VerdictLogger.error(this, "The source name of a joined table should not be called.");
		return null;
	}
}
