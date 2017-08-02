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
		return String.format("RANDOM() %% %d", mod);
	}

	@Override
	public String modOfHash(String col, int mod) {
		return String.format("mod(strtol(crc32(cast(%s as text)),16),%d)", col, mod);
	}
	
	@Override
	protected String randomNumberExpression(SampleParam param) {
		String expr = "RANDOM()";
		return expr;
	}

	@Override
	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("mod(cast(round(RANDOM()*%d) as integer), %d) AS %s", pcount, pcount, partitionColumnName());
	}
	
}
