package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ByPassVerdictUpdateQuery extends Query {

	public ByPassVerdictUpdateQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public ByPassVerdictUpdateQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		vc.getDbms().executeUpdate(queryString);
		return null;
	}

}
