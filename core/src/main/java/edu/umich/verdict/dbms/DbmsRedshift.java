package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsRedshift extends DbmsJDBC {

	public DbmsRedshift(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {		
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
		currentSchema = Optional.of("public");
	}
	
	@Override
	public String getQuoteString() {
		return "\"";
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
	public void createCatalog(String catalog) throws VerdictException {
        String sql = String.format("create schema if not exists %s", catalog);
        executeUpdate(sql);
    }

	@Override
	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("mod(cast(round(RANDOM()*%d) as integer), %d) AS %s", pcount, pcount, partitionColumnName());
	}
	
	@Override
    public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
        StringBuilder sql = new StringBuilder(1000);
        sql.append(String.format("insert into %s values ", tableName));
        sql.append("(");
        String with = "'";
        sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
        sql.append(")");
        executeUpdate(sql.toString());
    }
	
	@Override
	public void moveTable(TableUniqueName from, TableUniqueName to) throws VerdictException {
        VerdictLogger.debug(this, String.format("Moves table %s to table %s", from, to));
        String sql = String.format("DROP TABLE IF EXISTS %s; CREATE TABLE %s AS SELECT * FROM %s",to, to, from);
        dropTable(to);
        executeUpdate(sql);
        dropTable(from);
        VerdictLogger.debug(this, "Moving table done.");
    }
	
	@Override
	protected void attachUniformProbabilityToTempTable(SampleParam param, TableUniqueName temp) throws VerdictException {
        String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
        long total_size = SingleRelation.from(vc, param.getOriginalTable()).countValue();
        long sample_size = SingleRelation.from(vc, temp).countValue();

        ExactRelation withRand = SingleRelation.from(vc, temp)
                .select("*, " + String.format("%d / %d as %s", sample_size, total_size, samplingProbCol));
        
        String sql = String.format("drop table if exists %s ; create table %s as %s", param.sampleTableName(),
        		param.sampleTableName(), withRand.toSql());        
//        VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
//        VerdictLogger.debugPretty(this, Relation.prettyfySql(vc, sql), "  ");
        executeUpdate(sql);
    }
	
	@Override
	String composeUrl(String dbms, String host, String port, String schema, String user, String password) throws VerdictException {
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
	// this actually gets the schemas instead of database since in redshift database does not mater; schema matters.
	public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
//        return executeJdbcQuery("select nspname from pg_namespace WHERE datistemplate = false");
        return executeJdbcQuery("select nspname from pg_namespace");
    }
	
	@Override
	public void createMetaTablesInDMBS(
            TableUniqueName originalTableName,
            TableUniqueName sizeTableName,
            TableUniqueName nameTableName) throws VerdictException {
        VerdictLogger.debug(this, "Creates meta tables if not exist.");
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName)
                + " (schemaname TEXT, "
                + " tablename TEXT, "
                + " samplesize BIGINT, "
                + " originaltablesize BIGINT)";
        executeUpdate(sql);

        sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName)
                + " (originalschemaname TEXT, "
                + " originaltablename TEXT, "
                + " sampleschemaaname TEXT, "
                + " sampletablename TEXT, "
                + " sampletype TEXT, "
                + " samplingratio FLOAT, "
                + " columnnames TEXT)";
        executeUpdate(sql);

        VerdictLogger.debug(this, "Meta tables created.");
    }
	
}