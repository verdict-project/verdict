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
import edu.umich.verdict.relation.expr.OrderByExpr;
import edu.umich.verdict.relation.expr.SelectElem;

public class LimitedRelation extends ExactRelation {
	
	private ExactRelation source;
	
	private long limit;

	public LimitedRelation(VerdictContext vc, ExactRelation source, long limit) {
		super(vc);
		this.source = source;
		this.limit = limit;
	}
	
	public ExactRelation getSource() {
		return source;
	}

	@Override
	protected String getSourceName() {
		return getAliasName();
	}

	@Override
	public ApproxRelation approx() throws VerdictException {
		ApproxRelation a = new ApproxLimitedRelation(vc, source.approx(), limit);
		a.setAliasName(getAliasName());
		return a;
	}

	@Override
	protected ApproxRelation approxWith(Map<TableUniqueName, SampleParam> replace) {
		return null;
	}

	@Override
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append(source.toSql());
		sql.append(" LIMIT " + limit);
		return sql.toString();
	}

	@Override
	public List<SelectElem> getSelectList() {
		return source.getSelectList();
	}

	@Override
	public ColNameExpr partitionColumn() {
		ColNameExpr col = source.partitionColumn();
		col.setTab(getAliasName());
		return col;
	}

}
