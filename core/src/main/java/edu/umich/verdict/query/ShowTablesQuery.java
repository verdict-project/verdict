package edu.umich.verdict.query;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class ShowTablesQuery extends SelectQuery {

	public ShowTablesQuery(VerdictContext vc, String q) {
		super(vc, q);
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
