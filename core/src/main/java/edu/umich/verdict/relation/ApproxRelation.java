package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.CondModifier;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.TypeCasting;

public abstract class ApproxRelation extends Relation {

	public ApproxRelation(VerdictContext vc) {
		super(vc);
		approximate = true;
	}
	
	/*
	 * Approx
	 */
	
	public ApproxAggregatedRelation agg(Expr... functions) {
		return agg(Arrays.asList(functions));
	}
	
	public ApproxAggregatedRelation agg(List<Expr> functions) {
		return new ApproxAggregatedRelation(vc, this, functions);
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
	
	/**
	 * Properly scale all aggregation functions so that the final answers are correct.
	 * For ApproxAggregatedRelation: returns a AggregatedRelation instance whose result is approximately correct.
	 * For ApproxSingleRelation, ApproxJoinedRelation, and ApproxFilteredRelaation: returns
	 * a select statement from sample tables. The rewritten sql doesn't have much meaning unless used by ApproxAggregatedRelation. 
	 * @return
	 */
	public abstract ExactRelation rewrite();
	
	/**
	 * Computes an appropriate sampling probability for a particular aggregate function.
	 * @param f
	 * @return
	 */
	protected abstract double samplingProbabilityFor(FuncExpr f);
	
	/**
	 * Pairs of original table name and a sample table name. This function does not inspect subqueries.
	 * @return
	 */
	protected abstract Map<String, String> tableSubstitution();
	
	/*
	 * order by and limit
	 */
	
	public ApproxRelation orderby(String orderby) {
		String[] tokens = orderby.split(",");
		List<ColNameExpr> cols = new ArrayList<ColNameExpr>();
		for (String t : tokens) {
			cols.add(ColNameExpr.from(t));
		}
		return new ApproxOrderedRelation(vc, this, cols);
	}
	
	/*
	 * sql
	 */

	@Override
	protected String toSql() {
		return rewrite().toSql();
	}
	
	/*
	 * Helpers
	 */
	
	protected Expr exprWithTableNamesSubstituted(Expr expr, final Map<String, String> sub) {
		ExprModifier v = new ExprModifier() {
			public Expr call(Expr expr) {
				if (expr instanceof ColNameExpr) {
					ColNameExpr e = (ColNameExpr) expr;
					return new ColNameExpr(e.getCol(), sub.get(e.getTab()), e.getSchema());
				} else if (expr instanceof FuncExpr) {
					FuncExpr e = (FuncExpr) expr;
					return new FuncExpr(e.getFuncName(), visit(e.getExpr()));
				} else {
					return expr;
				}
			}
		};
		return v.visit(expr);
	}
	
	/**
	 * Returns a new condition in which old column names are replaced with the column names of sample tables. 
	 * @param cond
	 * @param sub Map of original table name and its substitution.
	 * @return
	 */
	protected Cond condWithTableNamesSubstituted(Cond cond, final Map<String, String> sub) {
		CondModifier v = new CondModifier() {
			ExprModifier v2 = new ExprModifier() {
				public Expr call(Expr expr) {
					if (expr instanceof ColNameExpr) {
						ColNameExpr e = (ColNameExpr) expr;
						return new ColNameExpr(e.getCol(), sub.get(e.getTab()), e.getSchema());
					} else {
						return expr;
					}
				}
			};
			
			public Cond call(Cond cond) {
				if (cond instanceof CompCond) {
					CompCond c = (CompCond) cond;
					return CompCond.from(v2.visit(c.getLeft()), v2.visit(c.getRight()), c.getOp());
				} else {
					return cond;
				}
			}			
		};
		return v.visit(cond);
	}

}
