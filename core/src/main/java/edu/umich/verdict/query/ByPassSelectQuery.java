package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;

public class ByPassSelectQuery extends SelectQuery {

	public ByPassSelectQuery(VerdictJDBCContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		if (vc.getDbms() instanceof DbmsJDBC) {
			rs = vc.getDbms().executeJdbcQuery(queryString);
		} else if (vc.getDbms() instanceof DbmsSpark) {
			df = vc.getDbms().executeSparkQuery(queryString);
		}
	}

}
