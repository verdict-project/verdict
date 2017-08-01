package edu.umich.verdict.dbms;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.exceptions.VerdictException;

public class DbmsRedshift extends DbmsJDBC {

	public DbmsRedshift(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
	}
	
	@Override
	public String getQuoteString() {
		return "`";
	}
	
	@Override
	protected String modOfRand(int mod) {
		return String.format("abs(rand(unix_timestamp())) %% %d", mod);
	}

	@Override
	public String modOfHash(String col, int mod) {
		return String.format("pmod(crc32(cast(%s as string)),%d)", col, mod);
	}
	
	@Override
	protected String randomNumberExpression(SampleParam param) {
		String expr = "rand(unix_timestamp())";
		return expr;
	}

	@Override
	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("pmod(round(rand(unix_timestamp())*%d), %d) AS %s", pcount, pcount, partitionColumnName());
	}
	
}
