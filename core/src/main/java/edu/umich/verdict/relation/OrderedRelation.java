package edu.umich.verdict.relation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.Expr;

public class OrderedRelation extends ExactRelation {
	
	protected ExactRelation source;
	
	protected List<ColNameExpr> orderby;
	
	protected OrderedRelation(VerdictContext vc, ExactRelation source, List<ColNameExpr> orderby) {
		super(vc);
		this.source = source;
		this.orderby = orderby;
		subquery = true;
	}

	@Override
	protected String getSourceName() {
		return getAliasName();
	}

	@Override
	public ApproxRelation approx() throws VerdictException {
		return new ApproxOrderedRelation(vc, source.approx(), orderby);
	}

	@Override
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return null;
	}

	@Override
	protected Map<Set<SampleParam>, Double> findSample(List<Expr> functions) {
		return new HashMap<Set<SampleParam>, Double>();
	}

	@Override
	protected String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append(source.toSql());
		sql.append(" ORDER BY ");
		sql.append(Joiner.on(", ").join(orderby));
		return sql.toString();
	}

}
