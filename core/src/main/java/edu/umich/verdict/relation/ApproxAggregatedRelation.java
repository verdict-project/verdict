package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.condition.IsCond;
import edu.umich.verdict.relation.condition.NullCond;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.CaseExpr;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprModifier;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;

public class ApproxAggregatedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private List<SelectElem> elems;

	public ApproxAggregatedRelation(VerdictContext vc, ApproxRelation source, List<SelectElem> elems) {
		super(vc);
		this.source = source;
		this.elems = elems;
	}
	
	public ApproxRelation getSource() {
		return source;
	}
	
	public List<SelectElem> getSelectList() {
		return elems;
	}

	@Override
	public ExactRelation rewrite() {
		List<SelectElem> scaled = new ArrayList<SelectElem>();
		List<TableUniqueName> stratifiedSampleTables = source.accumulateStratifiedSamples();
		for (SelectElem e : elems) {
			scaled.add(new SelectElem(transformForSingleFunction(e.getExpr(), stratifiedSampleTables), e.getAlias()));
		}
		ExactRelation r = new AggregatedRelation(vc, source.rewrite(), scaled);
		r.setAliasName(getAliasName());
		return r;
	}
	
	@Override
	protected Map<String, String> tableSubstitution() {
		return ImmutableMap.of();
	}
	
	@Override
	protected double samplingProbabilityFor(FuncExpr f) {
		return source.samplingProbabilityFor(f);
	}

	private Expr transformForSingleFunction(Expr f, final List<TableUniqueName> stratifiedSampleTables) {
		final Map<String, String> sub = source.tableSubstitution();
		
		ExprModifier v = new ExprModifier() {
			public Expr call(Expr expr) {
				if (expr instanceof FuncExpr) {
					// Take two different approaches:
					// 1. stratified samples: use the verdict_sampling_prob column (in each tuple).
					// 2. other samples: use either the uniform sampling probability or the ratio between the sample
					//    size and the original table size.
					
					FuncExpr f = (FuncExpr) expr;
					FuncExpr s = (FuncExpr) exprWithTableNamesSubstituted(expr, sub);
					double samplingProb = source.samplingProbabilityFor(f);
					
					if (f.getFuncName().equals(FuncExpr.FuncName.COUNT)) {
						Expr scale = ConstantExpr.from(1.0 / samplingProb);
						for (TableUniqueName t : stratifiedSampleTables) {
							scale = BinaryOpExpr.from(scale, new ColNameExpr(vc.samplingProbColName(), t.tableName), "/");
						}
						return FuncExpr.round(FuncExpr.sum(scale));
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
						String dbname = vc.getDbms().getName();
						if (dbname.equals("impala")) {
							return FuncExpr.round(
									BinaryOpExpr.from(new FuncExpr(
											FuncExpr.FuncName.IMPALA_APPROX_COUNT_DISTINCT, s.getExpr()),
											ConstantExpr.from(1.0 / samplingProb), "*"));
						} else {
							return FuncExpr.round(BinaryOpExpr.from(s, ConstantExpr.from(1.0 / samplingProb), "*"));
						}
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.SUM)) {
						Expr scale = ConstantExpr.from(1.0 / samplingProb);
						for (TableUniqueName t : stratifiedSampleTables) {
							scale = BinaryOpExpr.from(scale, new ColNameExpr(vc.samplingProbColName(), t.tableName), "/");
						}
						return FuncExpr.sum(BinaryOpExpr.from(s.getExpr(), scale, "*"));
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.AVG)) {
						Expr scale = ConstantExpr.from(1.0 / samplingProb);
						for (TableUniqueName t : stratifiedSampleTables) {
							scale = BinaryOpExpr.from(scale, new ColNameExpr(vc.samplingProbColName(), t.tableName), "/");
						}
						Expr sumEst = FuncExpr.sum(BinaryOpExpr.from(s.getExpr(), scale, "*"));
						// this count filters out the null expressions.
						Expr countEst = FuncExpr.sum(
								new CaseExpr(Arrays.<Cond>asList(new IsCond(s.getExpr(), new NullCond())),
											 Arrays.<Expr>asList(ConstantExpr.from("0"), scale)));
						return BinaryOpExpr.from(sumEst, countEst, "/");
					}
					else {		// expected not to be visited
						return s;
					}
				} else {
					return expr;
				}
			}
		};
		
		return v.visit(f);
	}

	@Override
	protected String sampleType() {
		return source.sampleType();
	}
	
	@Override
	protected List<TableUniqueName> accumulateStratifiedSamples() {
		return Arrays.asList();
	}

	@Override
	protected List<String> sampleColumns() {
		return source.sampleColumns();
	}
}
