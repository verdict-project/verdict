package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

<<<<<<< HEAD
import com.google.common.base.Joiner;
import com.google.common.base.Optional;

=======
>>>>>>> young/master
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsRedshift extends DbmsJDBC {

<<<<<<< HEAD
	public DbmsRedshift(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {		
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
		currentSchema = Optional.of("public");
	}
	
	@Override
	public String getQuoteString() {
		return "";
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
	
	
	
	@Override
	String composeUrl(String dbms, String host, String port, String schema, String user, String password) throws VerdictException {
=======
    public DbmsRedshift(VerdictContext vc, String dbName, String host, String port, String schema, String user,
            String password, String jdbcClassName) throws VerdictException {
        super(vc, dbName, host, port, schema, user, password, jdbcClassName);
    }

    @Override
    public String getQuoteString() {
        return "\\'";
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

    //	@Override
    //    protected String randomNumberExpression(SampleParam param) {
    //        Map<String, String> col2types = vc.getMeta().getColumn2Types(param.originalTable);
    //        Set<String> hashCols = new HashSet<String>();
    //        int precision = 3;
    //        int modValue = (int) Math.pow(10, precision);
    //
    //        for (Map.Entry<String, String> col2type : col2types.entrySet()) {
    //            String col = col2type.getKey();
    //            String type = col2type.getValue();
    //            if (type.toLowerCase().contains("char") || type.toLowerCase().contains("str")) {
    //                hashCols.add(String.format("fnv_hash((case when %s is null then cast(unix_timestamp() as string) else %s end))", col, col));
    //            } else if (type.toLowerCase().contains("time")) {
    //                hashCols.add(String.format("fnv_hash((case when %s is null then current_timestamp() else %s end))", col, col));
    //            } else {
    //                hashCols.add(String.format("fnv_hash((case when %s is null then unix_timestamp() else %s end))", col, col));
    //            }
    //        }
    //        String expr = "abs(fnv_hash("
    //                + Joiner.on(" + ").join(hashCols) 
    //                + String.format(" + unix_timestamp())) %% %d / %d", modValue, modValue);
    //        return expr;
    //    }

    @Override
    protected String randomPartitionColumn() {
        int pcount = partitionCount();
        return String.format("mod(cast(round(RANDOM()*%d) as integer), %d) AS %s", pcount, pcount, partitionColumnName());
    }

    @Override
    String composeUrl(String dbms, String host, String port, String schema, String user, String password) throws VerdictException {
>>>>>>> young/master
        StringBuilder url = new StringBuilder();
        url.append(String.format("jdbc:%s://%s:%s", dbms, host, port));

        if (schema != null) {
            url.append(String.format("/%s", schema));
        }

        if (!vc.getConf().ignoreUserCredentials() && user != null && user.length() != 0) {
            url.append(";");
            url.append(String.format("UID=%s", user));
        }
        if (!vc.getConf().ignoreUserCredentials() && password != null && password.length() != 0) {
            url.append(";");
            url.append(String.format("PWD=%s", password));
        }

        // set kerberos option if set
        if (vc.getConf().isJdbcKerberosSet()) {
            String value = vc.getConf().getJdbcKerberos();
            Pattern princPattern = Pattern.compile("(?<service>.*)/(?<host>.*)@(?<realm>.*)");
            Matcher princMatcher = princPattern.matcher(value);

            if (princMatcher.find()) {
                String service = princMatcher.group("service");
                String krbRealm = princMatcher.group("realm");
                String krbHost = princMatcher.group("host");

                url.append(String.format(";AuthMech=%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s;KrbAuthType=%s",
                        "1", krbRealm, krbHost, service, "2"));
            } else {
                VerdictLogger.error("Error: principal \"" + value + "\" could not be parsed.\n"
                        + "Make sure the principal is in the form service/host@REALM");
            }
        }

        // pass other configuration options.
        for (Map.Entry<String, String> pair : vc.getConf().getConfigs().entrySet()) {
            String key = pair.getKey();
            String value = pair.getValue();

            if (key.startsWith("verdict") || key.equals("user") || key.equals("password")) {
                continue;
            }

            url.append(String.format(";%s=%s", key, value));
        }

        return url.toString();
    }

    @Override
    public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName)  throws VerdictException {
        return executeJdbcQuery(String.format("SELECT \"column\",\"type\" FROM pg_table_def WHERE tablename = '%s'", tableUniqueName));
    }

    @Override
    public ResultSet getTablesInResultSet(String schema) throws VerdictException {        
        return executeJdbcQuery(String.format("SELECT DISTINCT tablename FROM pg_table_def WHERE schemaname = '%s'", schema));
    }

    @Override
    public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
        return executeJdbcQuery("SELECT datname FROM pg_database WHERE datistemplate = false");
    }

}
