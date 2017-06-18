package edu.umich.verdict.relation;

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
	
	/*
	 * Approx
	 */
	
	public ApproxRelation approx() throws VerdictException {
		Map<Set<SampleParam>, Double> candidates = source.findSample(exprsInSelectElems(elems));
		Map<TableUniqueName, SampleParam> best = chooseBest(candidates);
		return approxWith(best);
	}
	
	public ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return new ApproxAggregatedRelation(vc, source.approxWith(replace), elems);
	}

	public Map<Set<SampleParam>, Double> findSample(List<Expr> functions) {
		return new HashMap<Set<SampleParam>, Double>();
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
	
	protected String toSql() {
		StringBuilder sql = new StringBuilder();
		
		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
		List<Expr> groupby = groupsAndNextR.getLeft();
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString(vc) : "";
		
		sql.append(selectSql(groupby));
		sql.append(String.format(" FROM %s", sourceExpr(filtersAndNextR.getRight())));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		if (groupby.size() > 0) { sql.append(" GROUP BY "); sql.append(Joiner.on(", ").join(groupby)); }
		return sql.toString();
	}

}
