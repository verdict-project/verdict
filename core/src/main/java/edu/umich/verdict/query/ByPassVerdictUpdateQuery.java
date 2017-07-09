package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ByPassVerdictUpdateQuery extends Query {

	public ByPassVerdictUpdateQuery(VerdictJDBCContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		vc.getDbms().executeUpdate(queryString);
	}

}
