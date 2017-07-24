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
	
	@Override
	public String modOfHash(String col, int mod) {
		return String.format("pmod(crc32(cast(%s as string)),%d)", col, mod);
	}
	
	@Override
	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("pmod(round(rand(unix_timestamp())*%d), %d) AS %s", pcount, pcount, partitionColumnName());
	}
	
}
