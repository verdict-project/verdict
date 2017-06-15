package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

public class ExactRelation implements Relation {
	
	protected VerdictContext vc;
	
	protected TableUniqueName tableName;
	
	protected boolean derivedTable;

	protected ExactRelation(VerdictContext vc, TableUniqueName tableName) {
		this.vc = vc;
		this.tableName = tableName;
		derivedTable = true;
	}
	
	public static ExactRelation from(VerdictContext vc, TableUniqueName tableName) {
		ExactRelation r = new ExactRelation(vc, tableName);
		r.setDerivedTable(false);
		return r;
	}
	
	public static ExactRelation from(VerdictContext vc, String tableName) {
		ExactRelation r = new ExactRelation(vc, TableUniqueName.uname(vc, tableName));
		r.setDerivedTable(false);
		return r;
	}
	
	@Override
	public boolean isDerivedTable() {
		return derivedTable;
	}

	public void setDerivedTable(boolean a) {
		derivedTable = a;
	}
	
	@Override
	public boolean isApproximate() {
		return false;
	}

	@Override
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
	
	@Override
	public ResultSet collectResultSet() throws VerdictException {
		String sql = toSql();
		VerdictLogger.debug(this, "Written to: " + sql);
		return vc.getDbms().executeQuery(sql);
	}
	
	@Override
	public TableUniqueName getTableName() {
		return tableName;
	}
	
	protected String toSql() throws VerdictException {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * FROM " + tableName);
		return sql.toString();
	}
	
	public AggregatedRelation agg(Expr... functions) {
		return agg(Arrays.asList(functions));
	}
	
	public AggregatedRelation agg(List<Expr> functions) {
		return new AggregatedRelation(vc, this, functions);
	}
	
	public AggregatedSampleRelation approxAgg(List<Expr> functions) throws VerdictException {
		return agg(functions).approx();
	}
	
	public AggregatedSampleRelation approxAgg(Expr... functions) throws VerdictException {
		return agg(functions).approx();
	}
	

	@Override
	public long count() throws VerdictException {
		Relation r = agg(FuncExpr.count());
		List<List<Object>> rs = r.collect();
		return TypeCasting.toLong(rs.get(0).get(0));
	}
	
	public long approxCount() throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.count()).collect().get(0).get(0));
	}

	@Override
	public double sum(Expr expr) throws VerdictException {
		Relation r = agg(FuncExpr.sum(expr));
		List<List<Object>> rs = r.collect();
		return TypeCasting.toDouble(rs.get(0).get(0));
	}
	
	public double approxSum(Expr expr) throws VerdictException {
		return TypeCasting.toDouble(approxAgg(FuncExpr.sum(expr)).collect().get(0).get(0));
	}

	@Override
	public double avg(Expr expr) throws VerdictException {
		Relation r = agg(FuncExpr.avg(expr));
		List<List<Object>> rs = r.collect();
		return (Double) rs.get(0).get(0);
	}
	
	public double approxAvg(Expr expr) throws VerdictException {
		return TypeCasting.toDouble(approxAgg(FuncExpr.avg(expr)).collect().get(0).get(0));
	}
	
	@Override
	public long countDistinct(Expr expr) throws VerdictException {
		Relation r = agg(FuncExpr.countDistinct(expr));
		List<List<Object>> rs = r.collect();
		return TypeCasting.toLong(rs.get(0).get(0));
	}
	
	public long approxCountDistinct(Expr expr) throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(expr)).collect().get(0).get(0));
	}
	
	public long approxCountDistinct(String expr) throws VerdictException {
		return TypeCasting.toLong(approxAgg(FuncExpr.countDistinct(Expr.from(expr))).collect().get(0).get(0));
	}
	
}
