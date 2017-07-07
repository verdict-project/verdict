package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

public class AggregatedRelation extends ExactRelation {

	protected ExactRelation source;
	
	protected List<SelectElem> elems;
	
	private boolean includeGroupsInToSql = true;
	
	protected AggregatedRelation(VerdictContext vc, ExactRelation source, List<SelectElem> elems) {
		super(vc);
		this.source = source;
		this.elems = elems;
		subquery = true;
	}
	
	@Override
	protected String getSourceName() {
		return getAliasName();
	}
	
	public ExactRelation getSource() {
		return source;
	}
	
	public List<SelectElem> getAggList() {
		return elems;
	}
	
	public void setIncludeGroupsInToSql(boolean o) {
		includeGroupsInToSql = o;
	}
	
	/*
	 * Approx
	 */
	
	public ApproxRelation approx() throws VerdictException {
		// for each expression, we obtain pairs of sample candidates and the costs of using them. 
		List<List<SampleGroup>> candidates_list = new ArrayList<List<SampleGroup>>();
		for (int i = 0; i < elems.size(); i++) {
			List<SampleGroup> candidates = source.findSample(elems.get(i));
			candidates_list.add(candidates);
		}
		
		// check if any of them include sample tables. If no sample table is included, we do not approximate.
		boolean includeSample = false;
		for (List<SampleGroup> candidates : candidates_list) {
			for (SampleGroup g : candidates) {
				if (g.samplingProb() < 1.0) {
					includeSample = true;
					break;
				}
			}
			if (includeSample) break;
		}
		
		if (!includeSample) {
			return new NoApproxRelation(this);
		}
		
		// We test if we can consolidate those sample candidates so that the number of select statements is less than
		// the number of the expressions. In the worst case (e.g., all count-distinct), the number of select statements
		// will be equal to the number of the expressions. If the cost of running those select statements individually
		// is higher than the cost of running a single select statement using the original tables, samples are not used.
		SamplePlan plan = consolidate(candidates_list);
		VerdictLogger.debug(this, "The sample plan to use: ");
		VerdictLogger.debugPretty(this, plan.toPrettyString(), "  ");
		
		// we create multiple aggregated relations, which, when combined, can answer the user-submitted query.
		List<ApproxAggregatedRelation> individuals = new ArrayList<ApproxAggregatedRelation>();
		for (SampleGroup group : plan.getSampleGroups()) {
			List<SelectElem> elems = group.getElems();
			Set<SampleParam> samplesPart = group.sampleSet();
			individuals.add(new ApproxAggregatedRelation(vc, source.approxWith(attachTableMapping(samplesPart)), elems));
		}
		
		// join the results from those multiple relations (if there are more than one)
		ApproxRelation r = null;
		for (ApproxAggregatedRelation r1 : individuals) {
			if (r == null) {
				r = r1;
			} else {
				String ln = Relation.genTableAlias();
				String rn = Relation.genTableAlias();
				r.setAliasName(ln);
				r1.setAliasName(rn);
				if (r1.getSource() instanceof ApproxGroupedRelation) {
					List<ColNameExpr> groupby = ((ApproxGroupedRelation) r1.getSource()).getGroupby();
					List<Pair<Expr, Expr>> joincols = new ArrayList<Pair<Expr, Expr>>();
					for (ColNameExpr col : groupby) {
						joincols.add(Pair.of((Expr) new ColNameExpr(col.getCol(), ln), (Expr) new ColNameExpr(col.getCol(), rn)));
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
			List<ColNameExpr> groupby = ((ApproxGroupedRelation) firstSource).getGroupby();
			List<SelectElem> newElems = new ArrayList<SelectElem>();
			for (ColNameExpr g : groupby) {
				newElems.add(new SelectElem(new ColNameExpr(g.getCol(), individuals.get(0).getAliasName())));
			}
			for (SelectElem elem : elems) {
				newElems.add(new SelectElem(ConstantExpr.from(elem.getAlias()), elem.getAlias()));
			}
			r = new ApproxProjectedRelation(vc, r, newElems);
		}
		
		r.setAliasName(getAliasName());
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
		return new ApproxAggregatedRelation(vc, source.approxWith(replace), elems);
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
		
		sql.append(Joiner.on(", ").join(elems));
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

	@Override
	public List<SelectElem> getSelectList() {
		List<SelectElem> elems = new ArrayList<SelectElem>();
		
		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
		List<Expr> groupby = groupsAndNextR.getLeft();
		for (Expr g : groupby) {
			elems.add(new SelectElem(g));
		}
		
		elems.addAll(this.elems);		// agg list
		
		return elems;
	}

	@Override
	public List<SelectElem> selectElemsWithAggregateSource() {
		return elems;
	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col = source.partitionColumn();
		col.setTab(getAliasName());
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
		s.append(String.format("%s(%s) [%s]\n", this.getClass().getSimpleName(), getAliasName(), Joiner.on(", ").join(elems)));
		s.append(source.toStringWithIndent(indent + "  "));
		return s.toString();
	}
	
}
