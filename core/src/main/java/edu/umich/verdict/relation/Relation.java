package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.exceptions.VerdictUnexpectedMethodCall;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SelectElem;
import edu.umich.verdict.util.ResultSetConversion;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Both {@link ExactRelation} and {@link ApproxRelation} must extends this class.
 * @author Yongjoo Park
 *
 */
public abstract class Relation {
	
	protected VerdictContext vc;
	
	protected boolean subquery;
	
	protected boolean approximate;
	
	protected String alias;
	
	public Relation(VerdictContext vc) {
		this.vc = vc;
		this.subquery = false;
		this.approximate = false;
	}
	
	public boolean isSubquery() {
		return subquery;
	}
	
	public void setSubquery(boolean a) {
		this.subquery = a;
	}
	
	public boolean isApproximate() {
		return approximate;
	}
	
	public String getAliasName() {
		return alias;
	}
	
	public void setAliasName(String a) {
		alias = a;
	}
	
	/**
	 * Expression that would appear in sql statement.
	 * SingleSourceRelation: table name
	 * JoinedSourceRelation: join expression
	 * FilteredRelation: select * where condition from sourceExpr()
	 * AggregatedRelation: select groupby, agg where condition from sourceExpr()
	 * @return
	 * @throws VerdictUnexpectedMethodCall 
	 */
	protected abstract String toSql();
	
	/*
	 * Aggregation
	 */

	public abstract long count() throws VerdictException;

	public abstract double sum(String expr) throws VerdictException;
	
	public abstract double avg(String expr) throws VerdictException;
	
	public abstract long countDistinct(String expr) throws VerdictException;
	
	/*
	 * Collect results
	 */
	
	public ResultSet collectResultSet() throws VerdictException {
		String sql = toSql();
		VerdictLogger.info("The query to db: " + sql);
		return vc.getDbms().executeQuery(sql);
	}
	
	public List<List<Object>> collect() throws VerdictException {
		List<List<Object>> result = new ArrayList<List<Object>>();
		ResultSet rs = collectResultSet();
		try {
			int colCount = rs.getMetaData().getColumnCount();
			while (rs.next()) {
				List<Object> row = new ArrayList<Object>();	
				for (int i = 1; i <= colCount; i++) {
					row.add(rs.getObject(i));
				}
				result.add(row);
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return result;
	}
	
	public String collectAsString() {
		try {
			return ResultSetConversion.resultSetToString(collectResultSet());
		} catch (VerdictException e) {
			return StackTraceReader.stackTrace2String(e);
		}
	}
	
	@Override
	public String toString() {
		return this.getClass().getSimpleName();
	}
	
	/*
	 * Helpers
	 */
	
	protected List<Expr> exprsInSelectElems(List<SelectElem> elems) {
		List<Expr> exprs = new ArrayList<Expr>();
		for (SelectElem e : elems) {
			exprs.add(e.getExpr());
		}
		return exprs;
	}

}
