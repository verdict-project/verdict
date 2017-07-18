package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Represents aggregation operations on any source relation. This relation is expected to be a child of a
 * ProjectedRelation instance (which is always ensured when this instance is created from a sql statement).
 * @author Yongjoo Park
 *
 */
public class AggregatedRelation extends ExactRelation {

	protected ExactRelation source;
	
	protected List<Expr> aggs;
	
	private boolean includeGroupsInToSql = true;
	
	protected AggregatedRelation(VerdictContext vc, ExactRelation source, List<Expr> aggs) {
		super(vc);
		this.source = source;
		this.aggs = aggs;
		subquery = true;
	}
	
	@Override
	protected String getSourceName() {
		return getAlias();
	}
	
	public ExactRelation getSource() {
		return source;
	}
	
	public List<Expr> getAggList() {
		return aggs;
	}
	
	public void setIncludeGroupsInToSql(boolean o) {
		includeGroupsInToSql = o;
	}
	
	/*
	 * Approx
	 */
	
	/**
	 * if the source is a grouped relation, only a few sample types are allowed as a source of
	 * another aggregate relation.
	 * 1. stratified sample: the groupby column must be equal to the columns on which samples were built on.
	 *    the sample type of the aggregated relation will be "nosample" and the sample type will be "1.0".
	 * 2. universe sample: the groupby column must be equal to the columns on which samples where built on.
	 *    the sample type of the aggregated relation will be "universe" sampled on the same columns.
	 *    the sampling probability will also stays the same.
	 */
	@Override
	protected List<ApproxRelation> nBestSamples(Expr elem, int n) throws VerdictException {
		List<ApproxRelation> candidates = new ArrayList<ApproxRelation>();
		List<ApproxRelation> sourceCandidates = source.nBestSamples(elem, 10);
		for (ApproxRelation sc : sourceCandidates) {
			boolean eligible = false;
			
			if (sc instanceof ApproxGroupedRelation) {
				List<Expr> groupby = ((ApproxGroupedRelation) sc).getGroupby();
				List<String> strGroupby = new ArrayList<String>();
				for (Expr expr : groupby) {
					if (expr instanceof ColNameExpr) {
						strGroupby.add(((ColNameExpr) expr).getCol());
					}
				}
				
				String sampleType = sc.sampleType();
				List<String> sampleColumns = sc.sampleColumns();
				if (sampleType.equals("universe") && strGroupby.equals(sampleColumns)) {
					eligible = true;
				} else if (sampleType.equals("stratified") && strGroupby.equals(sampleColumns)) {
					eligible = true;
				}
			} else {
				eligible = true;
			}
			
			if (eligible) {
				ApproxRelation c = new ApproxAggregatedRelation(vc, sc, aggs);
				candidates.add(c);
			}
		}
		return candidates;
		
//		return Arrays.asList(approx());
	}
	
	public ApproxRelation approx() throws VerdictException {
		
		List<List<SampleGroup>> candidates_list = new ArrayList<List<SampleGroup>>();
		for (int i = 0; i < aggs.size(); i++) {
			Expr agg = aggs.get(i);
			List<ApproxRelation> candidates = source.nBestSamples(agg, 10);		// TODO: make this number (10) configurable.
			List<SampleGroup> sampleGroups = new ArrayList<SampleGroup>();
			for (ApproxRelation a : candidates) {
				sampleGroups.add(new SampleGroup(
						new ApproxAggregatedRelation(vc, a, Arrays.asList(agg)),
						Arrays.asList(agg)));
			}
			candidates_list.add(sampleGroups);
		}
		
//		// check if any of them include sample tables. If no sample table is included, we do not approximate.
//		boolean includeSample = false;
//		for (List<SampleGroup> candidates : candidates_list) {
//			for (SampleGroup g : candidates) {
//				if (g.samplingProb() < 1.0) {
//					includeSample = true;
//					break;
//				}
//			}
//			if (includeSample) break;
//		}
//		
//		if (!includeSample) {
//			return new NoApproxRelation(this);
//		}
		
		// We test if we can consolidate those sample candidates so that the number of select statements is less than
		// the number of the expressions. In the worst case (e.g., all count-distinct), the number of select statements
		// will be equal to the number of the expressions. If the cost of running those select statements individually
		// is higher than the cost of running a single select statement using the original tables, samples are not used.
		SamplePlan plan = consolidate(candidates_list);
		if (plan == null) {
			String msg = "No feasible sample plan is found.";
			VerdictLogger.error(this, msg);
			throw new VerdictException(msg);
		}
		VerdictLogger.debug(this, "The sample plan to use: ");
		VerdictLogger.debugPretty(this, plan.toPrettyString(), "  ");
		
//		// we create multiple aggregated relations, which, when combined, can answer the user-submitted query.
//		List<ApproxAggregatedRelation> individuals = new ArrayList<ApproxAggregatedRelation>();
//		for (SampleGroup group : plan.getSampleGroups()) {
//			List<Expr> elems = group.getElems();
//			Set<SampleParam> samplesPart = group.sampleSet();
//			individuals.add(new ApproxAggregatedRelation(vc, source.approxWith(attachTableMapping(samplesPart)), elems));
//		}
	
		List<ApproxAggregatedRelation> individuals = plan.getApproxRelations();
		
		// join the results from those multiple relations (if there are more than one)
		ApproxRelation r = null;
		for (ApproxAggregatedRelation r1 : individuals) {
			if (r == null) {
				r = r1;
			} else {
				String ln = r.getAlias();
				String rn = r1.getAlias();
//				r.setAliasName(ln);
//				r1.setAliasName(rn);
				if (r1.getSource() instanceof ApproxGroupedRelation) {
					List<Expr> groupby = ((ApproxGroupedRelation) r1.getSource()).getGroupby();
					List<Pair<Expr, Expr>> joincols = new ArrayList<Pair<Expr, Expr>>();
					for (Expr col : groupby) {
						// replace table names in internal colNameExpr
						joincols.add(Pair.of(col.withTableSubstituted(ln), col.withTableSubstituted(rn)));
//						joincols.add(Pair.of((Expr) new ColNameExpr(col.getCol(), ln), (Expr) new ColNameExpr(col.getCol(), rn)));
					}
//					r1.setIncludeGroupsInToSql(false);
					r = new ApproxJoinedRelation(vc, r, r1, joincols);
				} else {
					r = new ApproxJoinedRelation(vc, r, r1, null);
				}
			}
		}
		
		// if two or more tables are joined, groupby columns become ambiguous. So, we project out the groupby columns
		// in the joined relations.
		ApproxRelation firstSource = individuals.get(0).getSource();
		if (individuals.size() > 1 && (firstSource instanceof ApproxGroupedRelation)) {
			List<Expr> groupby = ((ApproxGroupedRelation) firstSource).getGroupby();
			List<SelectElem> newElems = new ArrayList<SelectElem>();
			for (Expr g : groupby) {
				newElems.add(new SelectElem(g.withTableSubstituted(individuals.get(0).getAlias())));
			}
			for (Expr elem : aggs) {
				newElems.add(new SelectElem(ConstantExpr.from(elem), Relation.genColumnAlias()));
			}
			r = new ApproxProjectedRelation(vc, r, newElems);
		}
		
		r.setAliasName(getAlias());
		return r;
	}

	private Map<TableUniqueName, SampleParam> attachTableMapping(Set<SampleParam> samplesPart) {
		Map<TableUniqueName, SampleParam> map = new HashMap<TableUniqueName, SampleParam>();
		for (SampleParam param : samplesPart) {
			map.put(param.originalTable, param);
		}
		return map;
	}

	public ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return new ApproxAggregatedRelation(vc, source.approxWith(replace), aggs);
	}
	
	/*
	 * Sql
	 */	
	
	protected String selectSql(List<Expr> groupby) {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		
		if (includeGroupsInToSql) {
			sql.append(Joiner.on(", ").join(groupby));
			if (groupby.size() > 0) sql.append(", ");
		}
		
		sql.append(Joiner.on(", ").join(aggs));
		return sql.toString();
	}
	
	@Deprecated
	protected String withoutSelectSql() {
		StringBuilder sql = new StringBuilder();
		
		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
		List<Expr> groupby = groupsAndNextR.getLeft();
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";
		
		sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		if (groupby.size() > 0) { sql.append(" GROUP BY "); sql.append(Joiner.on(", ").join(groupby)); }
		return sql.toString();
	}
	
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		
		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
		List<Expr> groupby = groupsAndNextR.getLeft();
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString() : "";
		
		sql.append(selectSql(groupby));
		sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		if (groupby.size() > 0) { sql.append(" GROUP BY "); sql.append(Joiner.on(", ").join(groupby)); }
		return sql.toString();
	}

//	@Override
//	public List<SelectElem> getSelectList() {
//		List<SelectElem> elems = new ArrayList<SelectElem>();
//		
//		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
//		List<Expr> groupby = groupsAndNextR.getLeft();
//		for (Expr g : groupby) {
//			elems.add(new SelectElem(g));
//		}
//		
//		elems.addAll(this.aggs);
//		
//		return elems;
//	}

//	@Override
//	public List<SelectElem> selectElemsWithAggregateSource() {
//		return aggs;
//	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col = source.partitionColumn();
//		col.setTab(getAliasName());
		return col;
	}

	@Override
	public List<ColNameExpr> accumulateSamplingProbColumns() {
		return Arrays.asList();
	}

	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAlias(), Joiner.on(", ").join(aggs)));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}
	
}
