package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ByPassSelectQuery extends SelectQuery {

	public ByPassSelectQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public ByPassSelectQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		return vc.getDbms().executeQuery(queryString);
	}

}
