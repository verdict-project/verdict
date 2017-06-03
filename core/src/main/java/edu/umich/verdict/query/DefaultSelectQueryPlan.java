package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class DefaultSelectQueryPlan extends SelectQuery {
	
	
	public DefaultSelectQueryPlan(String query, VerdictContext vc) {
		super(query, vc);
	}

	@Override
	public ResultSet compute() throws VerdictException {
		return vc.getDbms().executeQuery(queryString);
	}

}
