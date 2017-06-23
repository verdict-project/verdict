package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprVisitor;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.VerdictLogger;

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
	public String toSql() {
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

	@Override
	protected List<SampleGroup> findSample(SelectElem elem) {
		List<SampleGroup> candidates = new ArrayList<SampleGroup>();

		// Get all the samples
		List<Pair<SampleParam, TableUniqueName>> availableSamples = vc.getMeta().getSampleInfoFor(getTableName());
		// add a relation itself in case there's no available sample.
		availableSamples.add(Pair.of(asSampleParam(), getTableName()));
		
		// If there's no sample; we do not know the size of the original table. In this case, we simply assume the
		// size is 1M.
		double originalTableSize = 1e6;
		SampleSizeInfo si = vc.getMeta().getSampleSizeOf(availableSamples.get(0).getRight());
		if (si != null) {
			originalTableSize = si.originalTableSize;
		}
		
		for (Pair<SampleParam, TableUniqueName> p : availableSamples) {
			SampleSizeInfo sizeInfo = vc.getMeta().getSampleSizeOf(p.getRight());
			double sampleTableSize = originalTableSize;
			if (sizeInfo != null) {		// if not an original table
				sampleTableSize = (double) sizeInfo.sampleSize;
			}
			double samplingProb = samplingProb(p.getLeft(), elem.getExpr());
			
			if (samplingProb >= 0) {
				candidates.add(new SampleGroup(ImmutableSet.of(p.getLeft()), Arrays.asList(elem), samplingProb, sampleTableSize));
			}
		}
		
		return candidates;
	}
	
	/**
	 * Computes an effective sampling probability for a given sample and an aggregate expression to compute with the sample.
	 * A negative return value indicates that the sample must not be used.
	 * @param param
	 * @param expr
	 * @return
	 */
	private double samplingProb(SampleParam param, Expr expr) {
		if (expr instanceof FuncExpr) {
			Set<String> cols = new HashSet<String>(vc.getMeta().getColumnNames(getTableName()));
			
			FuncExpr fu = (FuncExpr) expr;
			String fcol = fu.getExprInString();
			if (fu.getExpr() instanceof ColNameExpr) {
				fcol = ((ColNameExpr) fu.getExpr()).getCol();
			}
			if (fu.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
				if (cols.contains(fcol)) {
					if (param.sampleType.equals("universe") && param.columnNames.contains(fcol)) {
						return param.samplingRatio;
					} else if (param.sampleType.equals("stratified") && param.columnNames.contains(fcol)) {
						return 1;
					} else if (param.sampleType.equals("nosample")) {
						return 1;
					} else {
						return -1;		// uniform random samples must not be used for COUNT-DISTINCT
					}
				} else {
					if (!param.sampleType.equals("nosample")) {
						return -1;		// no sampled table should be joined for count-distinct.
					} else {
						return 1;
					}
				}
			} else {
				if (param.sampleType.equals("stratified")) {
					VerdictLogger.warn(this, "Stratifeid samples are not supported yet.");
					return -1;		// stratified samples are not supported yet.
				} else {
					SampleSizeInfo size = vc.getMeta().getSampleSizeOf(param.sampleTableName());
					if (size == null) {
						return 1.0;		// the original table
					} else {
						return size.sampleSize / (double) size.originalTableSize;
					}
				}
			}
		} else {
			return param.samplingRatio;
		}
	}

	private double costOfSample(SampleParam param, List<Expr> aggExprs) {
		double cost_sum = 0;
		
//		return param.samplingRatio * param.
		
//		Set<String> cols = new HashSet<String>(vc.getMeta().getColumnNames(getTableName()));
//		
//		ExprVisitor<List<FuncExpr>> collectAggFuncs = new ExprVisitor<List<FuncExpr>>() {
//			private List<FuncExpr> seen = new ArrayList<FuncExpr>();
//			public List<FuncExpr> call(Expr expr) {
//				if (expr instanceof FuncExpr) {
//					seen.add((FuncExpr) expr);
//				}
//				return seen;
//			}
//		};
//		
//		for (Expr aggExpr : aggExprs) {
//			List<FuncExpr> funcs = collectAggFuncs.visit(aggExpr);
//			
//			for (FuncExpr f : funcs) {
//				String fcol = f.getExprInString();
//				if (f.getExpr() instanceof ColNameExpr) {
//					fcol = ((ColNameExpr) f.getExpr()).getCol();
//				}
//				if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT) && cols.contains(fcol)) {
//					if (param.sampleType.equals("universe")
//							&& param.columnNames.contains(fcol)) {
//						cost_sum += 50;
//					} else if (param.sampleType.equals("stratifeid")
//							&& param.columnNames.contains(fcol)) {
//						cost_sum += 40;
//					} else if (param.sampleType.equals("nosample")) {
//					} else {
//						cost_sum -= 100;
//					}
//				} else if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
//					if (param.sampleType.equals("nosample")) {
//					} else {
//						cost_sum -= 50;
//					}
//				} else {
//					if (param.sampleType.equals("nosample")) {
//					} else {
//						cost_sum += 10;
//					}
//				}
//			}
//		}
//		
		return cost_sum / aggExprs.size();
	}
	
	protected ApproxSingleRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		if (replace.containsKey(getTableName())) {
			ApproxSingleRelation a = ApproxSingleRelation.from(vc, replace.get(getTableName()));
			a.setAliasName(getAliasName());
			return a;
		} else {
			ApproxSingleRelation a = ApproxSingleRelation.asis(this);
			a.setAliasName(getAliasName());
			return a;
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
