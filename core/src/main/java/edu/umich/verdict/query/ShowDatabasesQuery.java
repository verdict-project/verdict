package edu.umich.verdict.query;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.dbms.DbmsJDBC;
import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.dbms.DbmsSpark2;
import edu.umich.verdict.exceptions.VerdictException;

public class ShowDatabasesQuery extends SelectQuery {

	public ShowDatabasesQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		if (vc.getDbms() instanceof DbmsJDBC) {
			rs = ((DbmsJDBC) vc.getDbms()).getDatabaseNamesInResultSet();
		} else if (vc.getDbms() instanceof DbmsSpark) {
			df = ((DbmsSpark) vc.getDbms()).getDatabaseNamesInDataFrame();
		} else if (vc.getDbms() instanceof DbmsSpark2) {
			ds = ((DbmsSpark2) vc.getDbms()).getDatabaseNamesInDataFrame();
		}
	}

}
