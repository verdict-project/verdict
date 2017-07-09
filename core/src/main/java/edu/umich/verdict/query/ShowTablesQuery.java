package edu.umich.verdict.query;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class ShowTablesQuery extends SelectQuery {

	public ShowTablesQuery(VerdictJDBCContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		if (!vc.getCurrentSchema().isPresent()) {
			VerdictLogger.info("No database schema selected; cannot show tables.");
			return;
		} else {
			String schemaName = vc.getCurrentSchema().get();
			
			if (vc.getDbms().isJDBC()) {
				rs = ((DbmsJDBC) vc.getDbms()).getTablesInResultSet(schemaName);
			} else if (vc.getDbms().isSpark()) {
				df = ((DbmsSpark) vc.getDbms()).getTablesInDataFrame(schemaName);
			}
		}
	}

}
