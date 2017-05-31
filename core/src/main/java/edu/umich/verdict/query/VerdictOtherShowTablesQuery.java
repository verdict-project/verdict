package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictOtherShowTablesQuery extends VerdictQuery {

	public VerdictOtherShowTablesQuery(String q, VerdictContext vc) {
		super(q, vc);
	}

	public VerdictOtherShowTablesQuery(VerdictQuery parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		if (!vc.getCurrentSchema().isPresent()) {
			VerdictLogger.info("No database schema selected; cannot show tables.");
			return null;
		} else {
			String schemaName = vc.getCurrentSchema().get();
			return vc.getDbms().getTableNames(schemaName);
		}
	}

}
