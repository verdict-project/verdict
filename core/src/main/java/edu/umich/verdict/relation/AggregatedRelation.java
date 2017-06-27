package edu.umich.verdict.relation;

import java.util.ArrayList;
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
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;

public class AggregatedRelation extends ExactRelation {

	protected ExactRelation source;
	
	protected List<SelectElem> elems;
	
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
		
		// We test if we can consolidate those sample candidates so that the number of select statements is less than
		// the number of the expressions. In the worst case (e.g., all count-distinct), the number of select statements
		// will be equal to the number of the expressions. If the cost of running those select statements individually
		// is higher than the cost of running a single select statement using the original tables, we do not use samples.
		SamplePlan plan = consolidate(candidates_list);
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
					r = new ApproxJoinedRelation(vc, r, r1, joincols);
				} else {
					r = new ApproxJoinedRelation(vc, r, r1, null);
				}
			}
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
		sql.append(Joiner.on(", ").join(groupby));
		if (groupby.size() > 0) sql.append(", ");
		sql.append(Joiner.on(", ").join(elems));
		return sql.toString();
	}
	
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

}
