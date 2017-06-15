package edu.umich.verdict.relation;

import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.Expr;

public class AggregatedRelation extends ExactRelation implements Relation {

	protected ExactRelation source;
	
	protected List<Expr> aggExpr;
	
	public AggregatedRelation(VerdictContext vc, ExactRelation source, List<Expr> aggExpr) {
		super(vc, source.getTableName());
		this.source = source;
		this.aggExpr = aggExpr;
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
		for (int i = 0; i < aggExpr.size(); i++) {
			if (i == 0) {
				sql.append(" " + aggExpr.get(i).toString(vc));
			} else {
				sql.append(", " + aggExpr.get(i).toString(vc));
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
