package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.CondModifier;
import edu.umich.verdict.relation.condition.OrCond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.VerdictLogger;

public class ApproxFilteredRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private Cond cond;

	public ApproxFilteredRelation(VerdictContext vc, ApproxRelation source, Cond cond) {
		super(vc);
		this.source = source;
		this.cond = cond;
		this.alias = source.alias;
	}

	public ApproxRelation getSource() {
		return source;
	}
	
	public Cond getFilter() {
		return cond;
	}

	@Override
	public ExactRelation rewriteForPointEstimate() {
		ExactRelation r = new FilteredRelation(vc, source.rewriteForPointEstimate(), condWithApprox(cond, tableSubstitution()));
		r.setAliasName(getAliasName());
		return r;
	}
	
	/**
	 * Returns a new condition in which old column names are replaced with the column names of sample tables. 
	 * @param cond
	 * @param sub Map of original table name and its substitution.
	 * @return
	 */
	private Cond condWithApprox(Cond cond, final Map<String, String> sub) {
		CondModifier v = new CondModifier() {
			ExprModifier v2 = new ExprModifier() {
				public Expr call(Expr expr) {
					if (expr instanceof ColNameExpr) {
						ColNameExpr e = (ColNameExpr) expr;
						return new ColNameExpr(e.getCol(), sub.get(e.getTab()), e.getSchema());
					} else if (expr instanceof SubqueryExpr) {
						Relation r = ((SubqueryExpr) expr).getSubquery();
						if (r instanceof ApproxRelation) {
							return SubqueryExpr.from(((ApproxRelation) r).rewrite());
						} else {
							VerdictLogger.warn(this, "An exact relation is found in an approximate query statement."
									+ " Mixing approximate relations with exact relations are not supported.");
							return expr;
						}
					} else {
						return expr;
					}
				}
			};
			
			public Cond call(Cond cond) {
				if (cond instanceof CompCond) {
					CompCond c = (CompCond) cond;
					return CompCond.from(v2.visit(c.getLeft()), c.getOp(), v2.visit(c.getRight()));
				} else {
					return cond;
				}
			}			
		};
		return v.visit(cond);
	}
	
	@Override
	public ExactRelation rewriteWithPartition() {
		// check if there's any comparison operations with subqueries.
		Pair<Cond, List<ApproxRelation>> modifiedCondWithRelToJoin = transformCondWithPartitionedRelations(cond, tableSubstitution());
		Cond modifiedCond = modifiedCondWithRelToJoin.getLeft();
		List<ApproxRelation> relToJoin = modifiedCondWithRelToJoin.getRight();
		
		ExactRelation joinedSource = source.rewriteWithPartition();
		for (ApproxRelation a : relToJoin) {
			List<Pair<Expr, Expr>> joinCol = Arrays.asList(Pair.<Expr, Expr>of(
					joinedSource.partitionColumn(),
					new ColNameExpr(partitionColumnName(), a.sourceTableName())));
			joinedSource = JoinedRelation.from(vc, joinedSource, a.rewriteWithPartition(), joinCol);
		}
		
		ExactRelation r = new FilteredRelation(vc, joinedSource, modifiedCond);
		r.setAliasName(getAliasName());
		return r;
	}
	
	/**
	 * 
	 * @param cond
	 * @return A pair of (1) the transformed condition and (2) a list of tables to be inner-joined on partition numbers.
	 * 		   The relations to be joined must have two selectElems: partition number and an aggregate column in order.
	 */
	private Pair<Cond, List<ApproxRelation>> transformCondWithPartitionedRelations(Cond cond, Map<String, String> sub) {
		CondModifierForSubsampling v = new CondModifierForSubsampling(sub);
		Cond modified = v.visit(cond);
		return Pair.of(modified, v.relationsToJoin());
	}
	
	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		return source.samplingProbabilityExprsFor(f);
	}

	@Override
	protected Map<String, String> tableSubstitution() {
		return source.tableSubstitution();
	}

	@Override
	protected String sampleType() {
		return source.sampleType();
	}
	
	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}

}

class CondModifierForSubsampling extends CondModifier {
	private List<ApproxRelation> compToRelations = new ArrayList<ApproxRelation>();
	
	private Map<String, String> sub;
	
	public CondModifierForSubsampling(Map<String, String> tableSub) {
		sub = tableSub;
	}
	
	public List<ApproxRelation> relationsToJoin() {
		return compToRelations;
	}
	
	ExprModifier v2 = new ExprModifier() {
		public Expr call(Expr expr) {
			if (expr instanceof ColNameExpr) {
				ColNameExpr e = (ColNameExpr) expr;
				return new ColNameExpr(e.getCol(), sub.get(e.getTab()), e.getSchema());
			} else if (expr instanceof SubqueryExpr) {
				Relation r = ((SubqueryExpr) expr).getSubquery();
				if (r instanceof ApproxAggregatedRelation) {
					// replace the subquery with the first aggregate expression.
					compToRelations.add((ApproxRelation) r);
					return new ColNameExpr(((ApproxAggregatedRelation) r).getSelectList().get(0).getAlias());
				} else if (r instanceof ApproxProjectedRelation) {
					// replace the subquery with the first select elem expression.
					compToRelations.add((ApproxRelation) r);
					return new ColNameExpr(((ApproxProjectedRelation) r).getSelectElems().get(0).getAlias());
				} else {
					VerdictLogger.warn(this, "An exact relation is found in an approximate query statement."
							+ " Mixing approximate relations with exact relations are not supported.");
					return expr;
				}
			} else {
				return expr;
			}
		}
	};
	
	public Cond call(Cond cond) {
		if (cond instanceof CompCond) {
			CompCond c = (CompCond) cond;
			return CompCond.from(v2.visit(c.getLeft()), c.getOp(), v2.visit(c.getRight()));
		} else {
			return cond;
		}
	}			
}
