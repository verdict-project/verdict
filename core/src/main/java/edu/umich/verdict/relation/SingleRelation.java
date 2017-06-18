package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.HashMap;
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
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprVisitor;
import edu.umich.verdict.relation.expr.FuncExpr;

public class SingleRelation extends ExactRelation {
	
	protected TableUniqueName tableName;
	
	protected static final String NOSAMPLE = "nosample";

	public SingleRelation(VerdictContext vc, TableUniqueName tableName) {
		super(vc);
		this.tableName = tableName;
		this.subquery = false;
	}
	
	public static SingleRelation from(VerdictContext vc, TableUniqueName tableName) {
		SingleRelation r = new SingleRelation(vc, tableName);
		return r;
	}
	
	public static SingleRelation from(VerdictContext vc, String tableName) {
		SingleRelation r = new SingleRelation(vc, TableUniqueName.uname(vc, tableName));
		return r;
	}
	
	public TableUniqueName getTableName() {
		return tableName;
	}
	
	@Override
	protected String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * FROM " + tableName);
		return sql.toString();
	}
	
	@Override
	protected String getSourceName() {
		return (alias == null)? tableName.toString() : getAliasName();
	}
	
	/*
	 * Approx
	 */
	
	@Override
	public ApproxRelation approx() throws VerdictException {
		// TODO Auto-generated method stub
		return null;
	}

	protected Map<Set<SampleParam>, Double> findSample(List<Expr> aggExprs) {
		Map<Set<SampleParam>, Double> scoredCandidate = new HashMap<Set<SampleParam>, Double>();

		// Get all the samples
		List<Pair<SampleParam, TableUniqueName>> availableSamples = vc.getMeta().getSampleInfoFor(getTableName());
		// add a relation itself in case there's no available sample.
		availableSamples.add(Pair.of(asSampleParam(), getTableName()));
		
		for (Pair<SampleParam, TableUniqueName> p : availableSamples) {
			scoredCandidate.put(ImmutableSet.of(p.getLeft()), scoreOfSample(p.getLeft(), aggExprs));
		}
		
		return scoredCandidate;
	}
	
	private double scoreOfSample(SampleParam param, List<Expr> aggExprs) {
		double score_sum = 0;
		
		Set<String> cols = new HashSet<String>(vc.getMeta().getColumnNames(getTableName()));
		
		ExprVisitor<List<FuncExpr>> collectAggFuncs = new ExprVisitor<List<FuncExpr>>() {
			private List<FuncExpr> seen = new ArrayList<FuncExpr>();
			public List<FuncExpr> call(Expr expr) {
				if (expr instanceof FuncExpr) {
					seen.add((FuncExpr) expr);
				}
				return seen;
			}
		};
		
		for (Expr aggExpr : aggExprs) {
			List<FuncExpr> funcs = collectAggFuncs.visit(aggExpr);
			
			for (FuncExpr f : funcs) {
				String fcol = f.getExprInString();
				if (f.getExpr() instanceof ColNameExpr) {
					fcol = ((ColNameExpr) f.getExpr()).getCol();
				}
				if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT) && cols.contains(fcol)) {
					if (param.sampleType.equals("universe")
							&& param.columnNames.contains(fcol)) {
						score_sum += 50;
					} else if (param.sampleType.equals("stratifeid")
							&& param.columnNames.contains(fcol)) {
						score_sum += 40;
					} else if (param.sampleType.equals("nosample")) {
					} else {
						score_sum -= 100;
					}
				} else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
					if (param.sampleType.equals("nosample")) {
					} else {
						score_sum -= 50;
					}
				} else {
					if (param.sampleType.equals("nosample")) {
					} else {
						score_sum += 10;
					}
				}
			}
		}
		
		return score_sum / aggExprs.size();
	}
	
	
//	protected Map<TableUniqueName, ApproxSingleRelation> chooseBest(Map<Set<SampleParam>, Integer> candidates) {
//		List<Pair<Set<ApproxSingleRelation>, Integer>> clist = TypeCasting.mapToList(candidates);
//		Collections.sort(clist, new Comparator<Pair<Set<ApproxSingleRelation>, Integer>>() {
//			public int compare(Pair<Set<ApproxSingleRelation>, Integer> o1, Pair<Set<ApproxSingleRelation>, Integer> o2) {
//				return o2.getValue() - o1.getValue();
//			}
//		});
//		
//		Map<TableUniqueName, ApproxSingleRelation> best = new HashMap<TableUniqueName, ApproxSingleRelation>();
//		for (ApproxSingleRelation s : clist.get(0).getLeft()) {
//			best.put(s.getOriginalTableName(), s);
//		}
//		return best;
//	}
	
	protected ApproxSingleRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		if (replace.containsKey(getTableName())) {
			return ApproxSingleRelation.from(vc, replace.get(getTableName()));
		} else {
			return ApproxSingleRelation.asis(this); 
		}
	}

	/*
	 *  Aggregation functions
	 */
	
//	protected String tableSourceExpr(SingleSourceRelation source) {
//		if (source.isDerivedTable()) {
//			return source.toSql();
//		} else {
//			return source.tableNameExpr();
//		}
//	}
	
	/*
	 * Helpers
	 */
	
	protected SampleParam asSampleParam() {
		return new SampleParam(getTableName(), NOSAMPLE, 1.0, null);
	}
	
}
