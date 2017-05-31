package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;

public class VerdictOtherShowDatabasesQuery extends VerdictQuery {

	public VerdictOtherShowDatabasesQuery(String q, VerdictContext vc) {
		super(q, vc);
	}

	public VerdictOtherShowDatabasesQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		return vc.getDbms().getDatabaseNames();
	}

}
