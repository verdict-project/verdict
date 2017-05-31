package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ByPassVerdictQuery extends VerdictQuery {

	public ByPassVerdictQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public ByPassVerdictQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		return vc.getDbms().executeQuery(queryString);
	}

}
