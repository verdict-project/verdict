package edu.umich.verdict.relation;

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
import edu.umich.verdict.relation.condition.AndCond;
import edu.umich.verdict.relation.condition.Cond;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;

public class GroupedRelation extends ExactRelation {

	protected ExactRelation source;
	
	protected List<ColNameExpr> groupby;

	public GroupedRelation(VerdictContext vc, ExactRelation source, List<ColNameExpr> groupby) {
		super(vc);
		this.source = source;
		this.groupby = groupby;
	}
	
	public ExactRelation getSource() {
		return source;
	}

	@Override
	protected String getSourceName() {
		return getAliasName();
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
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return new ApproxGroupedRelation(vc, source.approxWith(replace), groupby);
	}

	@Override
	protected Map<Set<SampleParam>, Double> findSample(List<Expr> functions) {
		return source.findSample(functions);
	}
	
	/*
	 * Aggs
	 */
	
	public ExactRelation counts() throws VerdictException {
		return agg(FuncExpr.count());
	}
	
	public ExactRelation avgs(String expr) throws VerdictException {
		return agg(FuncExpr.avg(expr));
	}
	
	public ExactRelation sums(String expr) throws VerdictException {
		return agg(FuncExpr.sum(expr));
	}
	
	public ExactRelation countDistincts(String expr) throws VerdictException {
		return agg(FuncExpr.countDistinct(expr));
	}
	
	public ApproxRelation approxCounts() throws VerdictException {
		return approxAgg(FuncExpr.count());
	}
	
	public ApproxRelation approxAvgs(String expr) throws VerdictException {
		return approxAgg(FuncExpr.avg(expr));
	}
	
	public ApproxRelation approxSums(String expr) throws VerdictException {
		return approxAgg(FuncExpr.sum(expr));
	}
	
	public ApproxRelation approxCountDistincts(String expr) throws VerdictException {
		return approxAgg(FuncExpr.countDistinct(expr));
	}
	
	
	/*
	 * Sql
	 */
	
	@Override
	protected String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * ");
		
		StringBuilder gsql = new StringBuilder();
		Pair<List<Expr>, ExactRelation> groupsAndNextR = allPrecedingGroupbys(this.source);
		for (Expr e : groupsAndNextR.getLeft()) {
			gsql.append(e);
		}
		
		Pair<Optional<Cond>, ExactRelation> filtersAndNextR = allPrecedingFilters(groupsAndNextR.getRight());
		String csql = (filtersAndNextR.getLeft().isPresent())? filtersAndNextR.getLeft().get().toString(vc) : "";
		
		sql.append(String.format(" FROM %s", sourceExpr(source)));
		if (csql.length() > 0) { sql.append(" WHERE "); sql.append(csql); }
		if (gsql.length() > 0) { sql.append(" GROUP BY "); sql.append(gsql); }
		return sql.toString();
	}

}
