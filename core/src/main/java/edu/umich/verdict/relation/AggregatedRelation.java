package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.exceptions.VerdictUnableToSupportException;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.ExprVisitor;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

public class AggregatedRelation extends ExactRelation implements Relation {

	protected ExactRelation source;
	
	protected List<Expr> aggExprs;
	
	private Map<Pair<FuncExpr.FuncName, String>, Integer> funcTypeAndProperSampleType = ImmutableMap.<Pair<FuncExpr.FuncName, String>, Integer>builder()
			.put(Pair.of(FuncExpr.FuncName.AVG, "uniform"), 30)
			.put(Pair.of(FuncExpr.FuncName.AVG, "universe"), 20)
			.put(Pair.of(FuncExpr.FuncName.AVG, "stratified"), 10)
			.put(Pair.of(FuncExpr.FuncName.COUNT, "uniform"), 30)
			.put(Pair.of(FuncExpr.FuncName.COUNT, "universe"), 20)
			.put(Pair.of(FuncExpr.FuncName.COUNT, "stratified"), 10)
			.put(Pair.of(FuncExpr.FuncName.SUM, "uniform"), 30)
			.put(Pair.of(FuncExpr.FuncName.SUM, "universe"), 20)
			.put(Pair.of(FuncExpr.FuncName.SUM, "stratified"), 10)
			.put(Pair.of(FuncExpr.FuncName.COUNT_DISTINCT, "universe"), 50)
			.put(Pair.of(FuncExpr.FuncName.COUNT_DISTINCT, "stratified"), 40)
			.build();
	
	public AggregatedRelation(VerdictContext vc, ExactRelation source, List<Expr> aggExpr) {
		super(vc, source.getTableName());
		this.source = source;
		this.aggExprs = aggExpr;
	}
	
	public AggregatedSampleRelation approx() throws VerdictException {
		List<Map<SampleRelation, Integer>> candidateSets = new ArrayList<Map<SampleRelation, Integer>>();
		for (Expr e : aggExprs) {
			candidateSets.add(candidatesForAgg(vc, e));
		}
		return new AggregatedSampleRelation(vc, bestSampleAmong(candidateSets), aggExprs);
	}
	
	/**
	 * Identifies an intersection of multiple candidates.
	 * @param scoredCandidates
	 * @return
	 * @throws VerdictException 
	 */
	private SampleRelation bestSampleAmong(List<Map<SampleRelation, Integer>> scoredCandidates) throws VerdictException {
		Map<SampleRelation, Integer> candidates = scoredCandidates.get(0);
		for (int i = 1; i < scoredCandidates.size(); i++) {
			candidates = intersectTwoCandidiates(scoredCandidates.get(i-1), scoredCandidates.get(i));
		}
		if (candidates.size() == 0) {
			throw new VerdictException("no proper sample table for this query.");
		}
		
		// sort
		List<Pair<SampleRelation, Integer>> cc = TypeCasting.mapToList(candidates);
		Comparator<Pair<SampleRelation, Integer>> comparator = new Comparator<Pair<SampleRelation, Integer>>() {
			public int compare(Pair<SampleRelation, Integer> left, Pair<SampleRelation, Integer> right) {
				return left.getRight() - right.getRight();
			}
		};
		Collections.sort(cc, comparator);
		return cc.get(cc.size()-1).getLeft();
	}
	
	private Map<SampleRelation, Integer> intersectTwoCandidiates(Map<SampleRelation, Integer> c1, Map<SampleRelation, Integer> c2) {
		Map<SampleRelation, Integer> inter = new HashMap<SampleRelation, Integer>();
		for (Map.Entry<SampleRelation, Integer> p : c2.entrySet()) {
			if (c1.containsKey(p.getKey())) {
				inter.put(p.getKey(), c1.get(p.getKey()) + p.getValue());
			}
		}
		return inter;
	}
	
	/**
	 * Identifies a set of proper sample relations for a given aggregate expression.
	 * @param aggExpr
	 * @return Set of a proper sample relation and its score for aggExpr 
	 * @throws VerdictException 
	 */
	private Map<SampleRelation, Integer> candidatesForAgg(VerdictContext vc, Expr aggExpr) throws VerdictException {
		ExprVisitor<List<FuncExpr>> collectAggFuncs = new ExprVisitor<List<FuncExpr>>() {
			private List<FuncExpr> seen = new ArrayList<FuncExpr>();
			public List<FuncExpr> call(Expr expr) throws VerdictException {
				if (expr instanceof FuncExpr) {
					seen.add((FuncExpr) expr);
				}
				return seen;
			}
		};
		List<FuncExpr> funcs = collectAggFuncs.visit(aggExpr);
		List<Pair<SampleParam, TableUniqueName>> sampleInfo = vc.getMeta().getSampleInfoFor(source.getTableName());
		Map<SampleRelation, Integer> candidates = new HashMap<SampleRelation, Integer>();
		for (int i = 0; i < funcs.size(); i++) {
			FuncExpr f = funcs.get(i);
			for (Pair<SampleParam, TableUniqueName> p : sampleInfo) {
				Pair<FuncExpr.FuncName, String> fnameAndSampleType = Pair.of(f.getFuncName(), p.getLeft().sampleType);
				if (funcTypeAndProperSampleType.containsKey(fnameAndSampleType)) {
					SampleRelation r = SampleRelation.from(vc, p.getLeft());
					int score = funcTypeAndProperSampleType.get(fnameAndSampleType);
					if (i == 0) {	// first function; simply add
						candidates.put(r, score);
					} else {		// second function; we only want intersection
						Map<SampleRelation, Integer> inter = new HashMap<SampleRelation, Integer>();
						if (candidates.containsKey(r)) {
							inter.put(r, candidates.get(r) + score);
						}
						candidates = inter;
					}
				}
			}
		}
		
		if (candidates.size() == 0) {
			VerdictLogger.warn("No sample can be used to approximate the expression: " + aggExpr.toString(vc));
			VerdictLogger.warn("This could be caused either because no samples were built or because count-distinct is combined with other aggregate functions.");
			VerdictLogger.warn("We will compute this query using the original table.");
			throw new VerdictUnableToSupportException(aggExpr.toString(vc));
		}
		
		return candidates;
	}
	
	protected String tableSourceExpr(ExactRelation source) throws VerdictException {
		if (source.isDerivedTable()) {
			return source.toSql();
		} else {
			return source.getTableName().toString();
		}
	}
	
	protected String selectSql() throws VerdictException {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT");
		for (int i = 0; i < aggExprs.size(); i++) {
			if (i == 0) {
				sql.append(" " + aggExprs.get(i).toString(vc));
			} else {
				sql.append(", " + aggExprs.get(i).toString(vc));
			}
		}
		return sql.toString();
	}
	
	protected String sourceSql() throws VerdictException {
		StringBuilder sql = new StringBuilder();
		sql.append("FROM ");
		sql.append(tableSourceExpr(source));
		return sql.toString();
	}
	
	protected String toSql() throws VerdictException {
		StringBuilder sql = new StringBuilder();
		sql.append(selectSql()); sql.append(" ");
		sql.append(sourceSql());
		return sql.toString();
	}

}
