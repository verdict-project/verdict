package edu.umich.verdict.dbms;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class DbmsHive extends DbmsImpala {

	public DbmsHive(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
	}
	
	@Override
	public String getQuoteString() {
		return "`";
	}

}
