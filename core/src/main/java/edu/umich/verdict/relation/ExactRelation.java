package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.ResultSetConversion;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

import com.google.common.base.Optional;

/**
 * Base class for exact relations (and any relational operations on them).
 * @author Yongjoo Park
 *
 */
public abstract class ExactRelation extends Relation {

	public ExactRelation(VerdictContext vc) {
		super(vc);
	}
	
//	/**
//	 * Returns an expression for a (possibly joined) table source.
//	 * SingleSourceRelation: a table name
//	 * JoinedRelation: a join expression
//	 * FilteredRelation: a full toSql()
//	 * ProjectedRelation: a full toSql()
//	 * AggregatedRelation: a full toSql()
//	 * GroupedRelation: a full toSql()
//	 * @return
//	 */
//	protected abstract String getSourceExpr();
	
	/**
	 * Returns a name for a (possibly joined) table source. It will be an alias name if the source is a derived table.
	 * @return
	 */
	protected abstract String getSourceName();
	
	/*
	 * Filtering
	 */
	
	public ExactRelation filter(Cond cond) throws VerdictException {
		return new FilteredRelation(vc, this, cond);
	}
	
	public ExactRelation filter(String cond) throws VerdictException {
		return filter(Cond.from(cond));
	}
	
	public ExactRelation where(String cond) throws VerdictException {
		return filter(cond);
	}
	
	/*
	 * Aggregation
	 */
	
	public AggregatedRelation agg(Object... elems) {
		return agg(Arrays.asList(elems));
	}
	
	public AggregatedRelation agg(List<Object> elems) {
		List<SelectElem> se = new ArrayList<SelectElem>();
		for (Object e : elems) {
			se.add(SelectElem.from(e.toString()));
		}
		return new AggregatedRelation(vc, this, se);
	}
	
	public AggregatedRelation agg(Expr... exprs) {
		return agg(Arrays.asList(exprs));
	}
	
	@Override
	public long count() throws VerdictException {
		return TypeCasting.toLong(agg(FuncExpr.count()).collect().get(0).get(0));
	}

	@Override
	public double sum(String expr) throws VerdictException {
		return TypeCasting.toDouble(agg(FuncExpr.sum(Expr.from(expr))).collect().get(0).get(0));
	}

	@Override
	public double avg(String expr) throws VerdictException {
		return TypeCasting.toDouble(agg(FuncExpr.avg(Expr.from(expr))).collect().get(0).get(0));
	}

	@Override
	public long countDistinct(String expr) throws VerdictException {
		return TypeCasting.toLong(agg(FuncExpr.countDistinct(Expr.from(expr))).collect().get(0).get(0));
	}
	
	public GroupedRelation groupby(String group) {
		String[] tokens = group.split(",");
		List<ColNameExpr> groups = new ArrayList<ColNameExpr>();
		for (String t : tokens) {
			groups.add(ColNameExpr.from(t));
		}
		return new GroupedRelation(vc, this, groups);
	}
	
	/*
	 * Approx Aggregation
	 */

	public ApproxRelation approxAgg(List<Object> elems) throws VerdictException {
		return agg(elems).approx();
	}
	
	public ApproxRelation approxAgg(Object... elems) throws VerdictException {
		return agg(elems).approx();
	}

	public long approxCount() throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.count()).collect().get(0).get(0));
	}

	public double approxSum(Expr expr) throws VerdictException {
		return TypeCasting.toDouble(approxAgg(FuncExpr.sum(expr)).collect().get(0).get(0));
	}

	public double approxAvg(Expr expr) throws VerdictException {
		return TypeCasting.toDouble(approxAgg(FuncExpr.avg(expr)).collect().get(0).get(0));
	}
	
	public long approxCountDistinct(Expr expr) throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(expr)).collect().get(0).get(0));
	}
	
	public long approxCountDistinct(String expr) throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(Expr.from(expr))).collect().get(0).get(0));
	}
	
	/*
	 * order by and limit
	 */
	
	public ExactRelation orderby(String orderby) {
		String[] tokens = orderby.split(",");
		List<ColNameExpr> cols = new ArrayList<ColNameExpr>();
		for (String t : tokens) {
			cols.add(ColNameExpr.from(t));
		}
		return new GroupedRelation(vc, this, cols);
	}
	
	/*
	 * Joins
	 */
	
	public JoinedRelation join(SingleRelation r, List<Pair<Expr, Expr>> joinColumns) {
		return JoinedRelation.from(vc, this, r, joinColumns);
	}
	
	public JoinedRelation join(SingleRelation r, Cond cond) throws VerdictException {
		return JoinedRelation.from(vc, this, r, cond);
	}
	
	public JoinedRelation join(SingleRelation r, String cond) throws VerdictException {
		return join(r, Cond.from(cond));
	}
	
	public JoinedRelation join(SingleRelation r) throws VerdictException {
		return join(r, (Cond) null);
	}	
	
	/*
	 * Transformation to ApproxRelation
	 */

	public abstract ApproxRelation approx() throws VerdictException;
	
	protected abstract ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace);

	/**
	 * Finds sets of samples that could be used for the table sources in a transformed approximate relation.
	 * Called on ProjectedRelation or AggregatedRelation, returns an empty set.
	 * Called on FilteredRelation, returns the result of its source.
	 * Called on JoinedRelation, combine the results of its two sources.
	 * Called on SingleRelation, finds a proper list of samples.
	 * Note that the return value's key (i.e., Set<ApproxSingleRelation>) holds a set of samples that point to all
	 * different relations. In other words, if this sql includes two tables, then the size of the set will be two, and
	 * the elements of the set will be the sample tables for those two tables. Multiple of such sets serve as condidates.
	 * @param functions
	 * @return A map from a candidate to score.
	 */
	protected abstract Map<Set<SampleParam>, Double> findSample(List<Expr> functions);
	
	protected Map<TableUniqueName, SampleParam> chooseBest(Map<Set<SampleParam>, Double> candidates) {
		Set<SampleParam> best = null;
		double max_score = Double.NEGATIVE_INFINITY;
		
		for (Map.Entry<Set<SampleParam>, Double> e : candidates.entrySet()) {
//			VerdictLogger.debug(this, String.format("candidate sample: %s, score: %f", e.getKey(), e.getValue()));
			if (e.getValue() > max_score) {
				best = e.getKey();
				max_score = e.getValue();
			}
		}
		
		Map<TableUniqueName, SampleParam> map = new HashMap<TableUniqueName, SampleParam>();
		if (best != null) {
			for (SampleParam s : best) {
				map.put(s.originalTable, s);
			}
		}
		
		return map;
	}
	
	/*
	 * Helpers
	 */

	/**
	 * 
	 * @param relation Starts to collect from this relation
	 * @return All found groupby expressions and the first relation that is not a GroupedRelation.
	 */
	protected Pair<List<Expr>, ExactRelation> allPrecedingGroupbys(ExactRelation r) {
		List<Expr> groupbys = new ArrayList<Expr>();
		ExactRelation t = r;
		while (true) {
			if (t instanceof GroupedRelation) {
				groupbys.addAll(((GroupedRelation) t).groupby);
				t = ((GroupedRelation) t).getSource();
			} else {
				break;
			}
		}
		return Pair.of(groupbys, t);
	}

	protected Pair<Optional<Cond>, ExactRelation> allPrecedingFilters(ExactRelation r) {
		Optional<Cond> c = Optional.absent();
		ExactRelation t = r;
		while (true) {
			if (t instanceof FilteredRelation) {
				if (c.isPresent()) {
					c = Optional.of((Cond) AndCond.from(c.get(), ((FilteredRelation) t).getFilter()));
				} else {
					c = Optional.of(((FilteredRelation) t).getFilter());
				}
				t = ((FilteredRelation) t).getSource();
			} else {
				break;
			}
		}
		return Pair.of(c, t);
	}

	protected String sourceExpr(ExactRelation source) {
		if (source instanceof SingleRelation) {
			return ((SingleRelation) source).getTableName().toString();
		} else if (source instanceof JoinedRelation) {
			return ((JoinedRelation) source).joinClause();
		} else {
			return source.toSql();
		}
	}
	
}
