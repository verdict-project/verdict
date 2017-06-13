package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class Relation {
	
	protected VerdictContext vc;
	
	protected TableUniqueName tableName;

	public Relation(VerdictContext vc, TableUniqueName tableName) {
		this.vc = vc;
		this.tableName = tableName;
	}
	
	public boolean isApproximate() {
		return false;
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
	
	public ResultSet collectResultSet() throws VerdictException {
		return vc.getDbms().executeQuery(toSql());
	}
	
	public String toSql() {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * FROM " + tableName);
		return sql.toString();
	}

}
