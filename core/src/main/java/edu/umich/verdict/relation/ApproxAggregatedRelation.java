package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
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
import edu.umich.verdict.relation.expr.OverClause;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.relation.expr.StarExpr;

public class ApproxAggregatedRelation extends ApproxRelation {
	
	private ApproxRelation source;
	
	private List<SelectElem> elems;

	private boolean includeGroupsInToSql = true;

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
	
	public void setIncludeGroupsInToSql(boolean o) {
		includeGroupsInToSql = o;
	}

	@Override
	public ExactRelation rewriteForPointEstimate() {
		ExactRelation newSource = source.rewriteForPointEstimate();
		
		List<SelectElem> scaled = new ArrayList<SelectElem>();
		List<ColNameExpr> samplingProbColumns = newSource.accumulateSamplingProbColumns();
		for (SelectElem e : elems) {
			scaled.add(new SelectElem(transformForSingleFunction(e.getExpr(), samplingProbColumns), e.getAlias()));
		}
		ExactRelation r = new AggregatedRelation(vc, newSource, scaled);
		r.setAliasName(getAliasName());
		((AggregatedRelation) r).setIncludeGroupsInToSql(includeGroupsInToSql);
		return r;
	}
	
	private final String partitionSizeAlias = "__vpsize";
	
	private final String groupSizeAlias = "__vgsize";
	
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		ExactRelation r = rewriteWithPartition();
//		List<SelectElem> selectElems = r.selectElemsWithAggregateSource();
		List<SelectElem> selectElems = ((AggregatedRelation) r).getAggList();
		
		// another wrapper to combine all subsampled aggregations.
		List<SelectElem> finalAgg = new ArrayList<SelectElem>();
		
		for (int i = 0; i < selectElems.size() - 1; i++) {	// excluding the last one which is psize
			SelectElem e = selectElems.get(i);
			ColNameExpr est = new ColNameExpr(e.getAlias(), r.getAliasName());
			ColNameExpr psize = new ColNameExpr(partitionSizeAlias, r.getAliasName());
			
			// average estimate
//			Expr meanEst = BinaryOpExpr.from(
//							FuncExpr.sum(BinaryOpExpr.from(est, psize, "*")),
//							FuncExpr.sum(psize), "/");
			Expr meanEst = FuncExpr.avg(est);
			Expr originalAggExpr = elems.get(i).getExpr(); 
			if (originalAggExpr instanceof FuncExpr) {
				if (((FuncExpr) originalAggExpr).getFuncName().equals(FuncExpr.FuncName.COUNT)
					|| ((FuncExpr) originalAggExpr).getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
					meanEst = FuncExpr.round(meanEst);
				}
			}
			finalAgg.add(new SelectElem(meanEst, e.getAlias()));

			// error estimation
			finalAgg.add(new SelectElem(
					BinaryOpExpr.from(
							BinaryOpExpr.from(FuncExpr.stddev(est), FuncExpr.sqrt(FuncExpr.avg(psize)), "*"),
							FuncExpr.sqrt(FuncExpr.sum(psize)),
							"/"),
					e.getAlias() + errColSuffix()));
		}
		
		/*
		 * Example input query:
		 * select category, avg(col)
		 * from t
		 * group by category
		 * 
		 * Transformed query:
		 * select category, sum(est * psize) / sum(psize) AS final_est
		 * from (
		 *   select category, avg(col) AS est, count(*) as psize
		 *   from t
		 *   group by category, verdict_partition) AS vt1
		 * group by category
		 * 
		 * where t1 was obtained by rewriteWithPartition().
		 */ 
		if (source instanceof ApproxGroupedRelation) {
			List<ColNameExpr> groupby = ((ApproxGroupedRelation) source).getGroupby();
			List<ColNameExpr> groupbyInNewSource = new ArrayList<ColNameExpr>();
			for (ColNameExpr g : groupby) {
				groupbyInNewSource.add(new ColNameExpr(g.getCol(), r.getAliasName()));
			}
			r = new GroupedRelation(vc, r, groupbyInNewSource);
		}
		
		r = new AggregatedRelation(vc, r, finalAgg);
		r.setAliasName(getAliasName());
		return r;
	}
	
	/**
	 * This relation must include partition numbers, and the answers must be scaled properly. Note that {@link ApproxRelation#rewriteWithSubsampledErrorBounds()}
	 * is used only for the statement including final error bounds; all internal manipulations must be performed by
	 * this method.
	 * @return
	 */
	protected ExactRelation rewriteWithPartition() {
		ExactRelation newSource = partitionedSource();
		
		// select list elements are scaled considering both sampling probabilities and partitioning for subsampling.
		List<SelectElem> scaledElems = new ArrayList<SelectElem>();
		List<ColNameExpr> samplingProbCols = newSource.accumulateSamplingProbColumns();
		List<ColNameExpr> groupby = new ArrayList<ColNameExpr>();
		if (source instanceof ApproxGroupedRelation) {
			groupby.addAll(((ApproxGroupedRelation) source).getGroupby());
		}
		
		final Map<String, String> sub = source.tableSubstitution();
		for (SelectElem e : elems) {
//			Expr scaled = transformForSingleFunction(e.getExpr(), samplingProbCols);
			Expr scaled = transformForSingleFunctionWithPartitionSize(e.getExpr(), samplingProbCols, groupby, newSource.partitionColumn(), sub);
			scaledElems.add(new SelectElem(scaled, e.getAlias()));
		}
		scaledElems.add(new SelectElem(FuncExpr.count(), partitionSizeAlias));
		ExactRelation r = new AggregatedRelation(vc, newSource, scaledElems);
		
		return r;
	}
	
	private ExactRelation partitionedSource() {
		if (source instanceof ApproxGroupedRelation) {
			return source.rewriteWithPartition();
		} else {
			return (new ApproxGroupedRelation(vc, source, Arrays.<ColNameExpr>asList())).rewriteWithPartition();
		}
	}
	
	@Override
	protected Map<String, String> tableSubstitution() {
		return ImmutableMap.of();
	}
	
	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		return Arrays.asList();
	}
	
	private Expr transformForSingleFunctionWithPartitionSize(
			Expr f,
			final List<ColNameExpr> samplingProbCols,
			List<ColNameExpr> groupby,
			final ColNameExpr partitionCol,
			final Map<String, String> tablesNamesSub) {
		
		final List<Expr> groupbyExpr = new ArrayList<Expr>();
		for (ColNameExpr c : groupby) {
			groupbyExpr.add((Expr) c);
		}
		
		ExprModifier v = new ExprModifier() {
			public Expr call(Expr expr) {
				if (expr instanceof FuncExpr) {
					// Take two different approaches:
					// 1. stratified samples: use the verdict_sampling_prob column (in each tuple).
					// 2. other samples: use either the uniform sampling probability or the ratio between the sample
					//    size and the original table size.
					
					FuncExpr f = (FuncExpr) expr;
					FuncExpr s = (FuncExpr) exprWithTableNamesSubstituted(expr, tablesNamesSub);
					List<Expr> samplingProbExprs = source.samplingProbabilityExprsFor(f);
					
					if (f.getFuncName().equals(FuncExpr.FuncName.COUNT)) {
						Expr est = FuncExpr.sum(scaleForSampling(samplingProbExprs));
						est = scaleWithPartitionSize(est, groupbyExpr, partitionCol);
						return est;
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
						String dbname = vc.getDbms().getName();
						Expr scale = scaleForSampling(samplingProbExprs);
						Expr est = null;
						
						if (dbname.equals("impala")) {
							est = new FuncExpr(FuncExpr.FuncName.IMPALA_APPROX_COUNT_DISTINCT, s.getUnaryExpr());
						} else {
							est = new FuncExpr(FuncExpr.FuncName.COUNT_DISTINCT, s.getUnaryExpr());
						}
						
						est = BinaryOpExpr.from(est, scale, "*");
						if (sampleType().equals("universe")) {
							est = scaleWithPartitionSize(est, groupbyExpr, partitionCol);
						}
						return est;
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.SUM)) {
						Expr est = scaleForSampling(samplingProbExprs);
						est = FuncExpr.sum(BinaryOpExpr.from(s.getUnaryExpr(), est, "*"));
						est = scaleWithPartitionSize(est, groupbyExpr, partitionCol);
						return est;
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.AVG)) {
						Expr scale = scaleForSampling(samplingProbExprs);
						Expr sumEst = FuncExpr.sum(BinaryOpExpr.from(s.getUnaryExpr(), scale, "*"));
						// this count-est filters out the null expressions.
						Expr countEst = countNotNull(s.getUnaryExpr(), scale);
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
	
	private Expr scaleForSampling(List<Expr> samplingProbCols) {
		Expr scale = ConstantExpr.from(1.0);
		for (Expr c : samplingProbCols) {
			scale = BinaryOpExpr.from(scale, c, "/");
		}
		return scale;
	}
	
	private Expr scaleWithPartitionSize(Expr expr, List<Expr> groupby, ColNameExpr partitionCol) {
//		Expr scaled = BinaryOpExpr.from(expr, FuncExpr.count(), "/");
		Expr scaled = BinaryOpExpr.from(expr, new FuncExpr(FuncExpr.FuncName.AVG, FuncExpr.count(), new OverClause(groupby)), "/");
		scaled = BinaryOpExpr.from(scaled, new FuncExpr(FuncExpr.FuncName.SUM, FuncExpr.count(), new OverClause(groupby)), "*");
		return scaled;
	}

	private Expr transformForSingleFunction(Expr f, final List<ColNameExpr> samplingProbCols) {
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
					List<Expr> samplingProbExprs = source.samplingProbabilityExprsFor(f);
					
					if (f.getFuncName().equals(FuncExpr.FuncName.COUNT)) {
						Expr scale = scaleForSampling(samplingProbExprs);
						Expr est = FuncExpr.sum(scale);
						return FuncExpr.round(est);
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
						String dbname = vc.getDbms().getName();
						Expr scale = scaleForSampling(samplingProbExprs);
						if (dbname.equals("impala")) {
							return FuncExpr.round(
									BinaryOpExpr.from(new FuncExpr(
											FuncExpr.FuncName.IMPALA_APPROX_COUNT_DISTINCT, s.getUnaryExpr()),
											scale, "*"));
						} else {
							return FuncExpr.round(BinaryOpExpr.from(s, scale, "*"));
						}
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.SUM)) {
						Expr scale = scaleForSampling(samplingProbExprs);
						Expr est = FuncExpr.sum(BinaryOpExpr.from(s.getUnaryExpr(), scale, "*"));
						return est;
					}
					else if (f.getFuncName().equals(FuncExpr.FuncName.AVG)) {
						Expr scale = scaleForSampling(samplingProbExprs);
						Expr sumEst = FuncExpr.sum(BinaryOpExpr.from(s.getUnaryExpr(), scale, "*"));
						// this count-est filters out the null expressions.
						Expr countEst = countNotNull(s.getUnaryExpr(), scale);
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
	
	private FuncExpr countNotNull(Expr nullcheck, Expr expr) {
		return FuncExpr.sum(
				new CaseExpr(Arrays.<Cond>asList(new IsCond(nullcheck, new NullCond())),
						     Arrays.<Expr>asList(ConstantExpr.from("0"), expr)));
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
