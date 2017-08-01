package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.CondModifier;
import edu.umich.verdict.relation.condition.CondVisitor;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.ExprVisitor;
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
		r.setAlias(getAlias());
		return r;
	}
	
	/**
	 * Returns a new condition in which old column names are replaced with the column names of sample tables. 
	 * @param cond
	 * @param sub Map of original table name and its substitution.
	 * @return
	 */
	private Cond condWithApprox(Cond cond, final Map<TableUniqueName, String> sub) {
		CondModifier v = new CondModifier() {
//			ExprModifier v2 = new ExprModifier() {
//				public Expr call(Expr expr) {
//					if (expr instanceof ColNameExpr) {
//						ColNameExpr e = (ColNameExpr) expr;
//						TableUniqueName old = TableUniqueName.uname(e.getSchema(), e.getTab());
//						if (sub.containsKey(old)) {
//							TableUniqueName rep = sub.get(old);
//							return new ColNameExpr(e.getCol(), rep.getTableName(), rep.getSchemaName());
//						} else {
//							return expr;
//						}
//						
//					} else if (expr instanceof SubqueryExpr) {
//						Relation r = ((SubqueryExpr) expr).getSubquery();
//						if (r instanceof ApproxRelation) {
//							return SubqueryExpr.from(((ApproxRelation) r).rewrite());
//						} else {
//							VerdictLogger.warn(this, "An exact relation is found in an approximate query statement."
//									+ " Mixing approximate relations with exact relations are not supported.");
//							return expr;
//						}
//					} else {
//						return expr;
//					}
//				}
//			};
			
			public Cond call(Cond cond) {
				if (cond instanceof CompCond) {
					CompCond c = (CompCond) cond;
					return CompCond.from(
							exprWithTableNamesSubstituted(c.getLeft(), sub),
							c.getOp(),
							exprWithTableNamesSubstituted(c.getRight(), sub));
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
		
		ApproxRelation joinedSource = source;
		String leftmostAlias = source.getAlias();
		for (ApproxRelation a : relToJoin) {
			List<Pair<Expr, Expr>> joincond = new ArrayList<Pair<Expr, Expr>>();
			ColNameExpr leftcol = new ColNameExpr(source.partitionColumnName(), leftmostAlias);
			ColNameExpr rightcol = new ColNameExpr(a.partitionColumnName(), a.getAlias());
			joincond.add(Pair.<Expr, Expr>of(leftcol, rightcol));
			joinedSource = new ApproxJoinedRelation(vc, joinedSource, a, joincond);
		}
		
//		ExactRelation joinedSource = source.rewriteWithPartition();
//		for (ApproxRelation a : relToJoin) {
//			List<Pair<Expr, Expr>> joinCol = Arrays.asList(Pair.<Expr, Expr>of(
//					joinedSource.partitionColumn(),
//					new ColNameExpr(partitionColumnName(), a.sourceTableName())));
//			joinedSource = JoinedRelation.from(vc, joinedSource, a.rewriteWithPartition(), joinCol);
//		}
		
		ExactRelation newSource = joinedSource.rewriteWithPartition();
		ExactRelation r = new FilteredRelation(vc, newSource, modifiedCond);
		r.setAlias(getAlias());
		return r;
	}
	
	/**
	 * 
	 * @param cond
	 * @return A pair of (1) the transformed condition and (2) a list of tables to be inner-joined on partition numbers.
	 * 		   The relations to be joined must have two selectElems: partition number and an aggregate column in order.
	 */
	private Pair<Cond, List<ApproxRelation>> transformCondWithPartitionedRelations(Cond cond, Map<TableUniqueName, String> sub) {
		CondModifierForSubsampling v = new CondModifierForSubsampling(sub);
		Cond modified = v.visit(cond);
		return Pair.of(modified, v.relationsToJoin());
	}
	
	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		return source.samplingProbabilityExprsFor(f);
	}

	@Override
	protected Map<TableUniqueName, String> tableSubstitution() {
		return source.tableSubstitution();
	}

	@Override
	public String sampleType() {
		return source.sampleType();
	}
	
	@Override
	public double cost() {
		CondVisitor<Double> v1 = new CondVisitor<Double>() {
			double cond_cost = 0;
			
			ExprVisitor<Double> v2 = new ExprVisitor<Double>() {
				double subquery_cost = 0;
				@Override
				public Double call(Expr expr) {
					if (expr instanceof SubqueryExpr) {
						Relation r = ((SubqueryExpr) expr).getSubquery();
						if (r instanceof ApproxRelation) {
							subquery_cost += ((ApproxRelation) r).cost();
						}
					}
					return subquery_cost;
				}
			};
			
			@Override
			public Double call(Cond cond) {
				if (cond instanceof CompCond) {
					cond_cost += v2.visit(((CompCond) cond).getLeft());
					cond_cost += v2.visit(((CompCond) cond).getRight());
				}
				return cond_cost;
			}
		};
		
		double cost = v1.visit(cond);
		return cost + source.cost();
	}
	
	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}

	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(), cond.toString()));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}
	
	@Override
	public boolean equals(ApproxRelation o) {
		if (o instanceof ApproxFilteredRelation) {
			if (source.equals(((ApproxFilteredRelation) o).source)) {
				if (cond.equals(((ApproxFilteredRelation) o).cond)) {
					return true;
				}
			}
		}
		return false;
	}
	
	@Override
	public double samplingProbability() {
		return source.samplingProbability();
	}

    @Override
    protected boolean doesIncludeSample() {
        return source.doesIncludeSample();
    }

}

// modifies the subquery expression in comparison condition
class CondModifierForSubsampling extends CondModifier {
	private List<ApproxRelation> compToRelations = new ArrayList<ApproxRelation>();		// relations to compare
	
	private Map<TableUniqueName, String> sub;
	
	private ExprModifier v2;
	
	public CondModifierForSubsampling(Map<TableUniqueName, String> tableSub) {
		sub = tableSub;
		v2 = new TableNameReplacerInExpr(sub) {
			@Override
			protected Expr replaceSubqueryExpr(SubqueryExpr expr) {
				Relation r = expr.getSubquery();
				if (r instanceof ApproxAggregatedRelation) {
					// replace the subquery with the first aggregate expression.
//					compToRelations.add((ApproxRelation) r);
//					return new ColNameExpr(((ApproxAggregatedRelation) r).getSelectList().get(0).getAlias());
					VerdictLogger.warn(this, "A non-projected relation is not expected to be found in a subquery.");
					return expr;
				} else if (r instanceof ApproxProjectedRelation) {
					// replace the subquery with the first select elem expression.
					compToRelations.add((ApproxRelation) r);
					return new ColNameExpr(((ApproxProjectedRelation) r).getSelectElems().get(0).getAlias());
				} else {
					VerdictLogger.warn(this, "An exact relation is found in an approximate query statement."
							+ " Mixing approximate relations with exact relations are not supported.");
					return expr;
				}
			}
		};
	}
	
	public List<ApproxRelation> relationsToJoin() {
		return compToRelations;
	}
	
	public Cond call(Cond cond) {
		if (cond instanceof CompCond) {
			CompCond c = (CompCond) cond;
			return CompCond.from(v2.visit(c.getLeft()), c.getOp(), v2.visit(c.getRight()));
		} else {
			return cond;
		}
	}		
}
