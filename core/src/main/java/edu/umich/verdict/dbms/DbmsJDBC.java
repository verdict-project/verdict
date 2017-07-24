package edu.umich.verdict.dbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public abstract class DbmsJDBC extends Dbms {

	protected final Connection conn;
	
	protected Statement stmt;		// created Statements must be registered here.
	
	public Connection getDbmsConnection() {
		return conn;
	}
	
	public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
		return executeJdbcQuery("show databases");
	}

//	@Override
//	public List<String> getTables(String schemaName) throws VerdictException {
//		try {
//			String[] types = {"TABLE", "VIEW"};
//			ResultSet rs = conn.getMetaData().getTables(schemaName, null, "%", types);
//			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
//			columnMap.put(1, 3);	// table name
//			columnMap.put(2, 4);	// table type
//			return new VerdictResultSet(rs, null, columnMap);
//		} catch (SQLException e) {
//			throw new VerdictException(e);
//		}
//	}

	protected ResultSet rs;
	
	/**
	 * Copy constructor for not sharing the underlying statement.
	 * @param another
	 */
	public DbmsJDBC(Dbms another) {
		super(another);
		if (another instanceof DbmsJDBC) {
			conn = ((DbmsJDBC) another).conn;
		} else {
			conn = null;
		}
		stmt = null;
	}
	
	protected DbmsJDBC(VerdictContext vc,
				       String dbName,
					   String host,
					   String port,
					   String schema,
					   String user,
					   String password,
					   String jdbcClassName) throws VerdictException {
		super(vc, dbName);
		currentSchema = Optional.fromNullable(schema);
		String url = composeUrl(dbName,
								host,
								port,
								schema,
								user,
								password);
		conn = makeDbmsConnection(url, jdbcClassName);
	}
	
	public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName)  throws VerdictException {
		return executeJdbcQuery(String.format("describe %s", tableUniqueName));
	}
	
	@Override
	public Set<String> getDatabases() throws VerdictException {
		Set<String> databases = new HashSet<String>();
		try {
			ResultSet rs = getDatabaseNamesInResultSet();
			while (rs.next()) {
				databases.add(rs.getString(1));
			}
		} catch (SQLException e) {
			throw new VerdictException(StackTraceReader.stackTrace2String(e));
		}
		return databases;
	}
	
	public ResultSet getTablesInResultSet(String schema) throws VerdictException {
		return executeJdbcQuery("show tables in " + schema);
	}

	@Override
	public List<String> getTables(String schema) throws VerdictException {
		List<String> tables = new ArrayList<String>();
		try {
			ResultSet rs = getTablesInResultSet(schema);
			while (rs.next()) {
				String table = rs.getString(1);
				tables.add(table);
			}
		} catch (SQLException e) {
			VerdictLogger.error(this, "Failed to access the database: " + schema);
			throw new VerdictException(e);
		}
		return tables;
	}
	
	@Override
	public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
		Map<String, String> col2type = new LinkedHashMap<String, String>();
		try {
			ResultSet rs = describeTableInResultSet(table);
			while (rs.next()) {
				String column = rs.getString(1);
				String type = rs.getString(2);
				col2type.put(column, type);
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return col2type;
	}
	
//	public void createMetaTablesInDMBS(
//			TableUniqueName originalTableName,
//			TableUniqueName sizeTableName,
//			TableUniqueName nameTableName) throws VerdictException {
//		VerdictLogger.debug(this, "Creates meta tables if not exist.");
//		
//		String sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName)
//								+ " (originalschemaname VARCHAR(50), "
//								+ " originaltablename VARCHAR(50), "
//								+ " sampleschemaaname VARCHAR(50), "
//								+ " sampletablename VARCHAR(50), "
//								+ " sampletype VARCHAR(20), "
//								+ " samplingratio DOUBLE, "
//								+ " columnnames VARCHAR(200))";
//		executeUpdate(sql);
//		
//		// sample info
//		sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName)
//						+ " (schemaname VARCHAR(50), "
//						+ " tablename VARCHAR(50), "
//						+ " samplesize BIGINT, "
//						+ " originaltablesize BIGINT)";
//		executeUpdate(sql);
//		
//		VerdictLogger.debug(this, "Finished createing meta tables if not exist.");
//	}
	
	String composeUrl(String dbms, String host, String port, String schema, String user, String password) throws VerdictException {
		StringBuilder url = new StringBuilder();
		url.append(String.format("jdbc:%s://%s:%s", dbms, host, port));
		
		if (schema != null) {
			url.append(String.format("/%s", schema));
		}

		boolean isFirstParam = true;
		if (user != null && user.length() != 0) {
			url.append((isFirstParam)? "?" : "&");
			url.append(String.format("user=%s", user));
			isFirstParam = false;
		}
		if (password != null && password.length() != 0) {
			url.append((isFirstParam)? "?" : "&");
			url.append(String.format("password=%s", password));
			isFirstParam = false;
		}
		
		for (Map.Entry<String, String> pair : vc.getConf().getConfigs().entrySet()) {
			String key = pair.getKey();
			String value = pair.getValue();
			
			if (key.startsWith("verdict") || key.equals("user") || key.equals("password")) {
				continue;
			}
			
			if (key.equals("principal")) {
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
			else {
				url.append(String.format(";%s=%s", key, value));
			}
		}
		
		return url.toString();
	}
	
	protected Connection makeDbmsConnection(String url, String className) throws VerdictException  {
		try {
			Class.forName(className);
			VerdictLogger.debug(this, "JDBC connection string: " + url);
			Connection conn = DriverManager.getConnection(url);
			return conn;
		} catch (ClassNotFoundException | SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public long getTableSize(TableUniqueName tableName) throws VerdictException {
		ResultSet rs;
		long cnt = 0;
		try {
			String sql = String.format("SELECT COUNT(*) FROM %s", tableName);
			rs = executeJdbcQuery(sql);
			while(rs.next()) {cnt = rs.getLong(1);	}
			rs.close();
		} catch (SQLException e) {
			throw new VerdictException(StackTraceReader.stackTrace2String(e));
		}
		return cnt;
	}

	public boolean execute(String sql) throws VerdictException {
		createStatementIfNotExists();
		boolean result = false;
		try {
			result = stmt.execute(sql);
			if (result) {
				rs = stmt.getResultSet();
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return result;
	}
	
	public void executeUpdate(String query) throws VerdictException { 
		createStatementIfNotExists();
		try {
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public Statement createStatement() throws VerdictException {
		try {
			if (stmt != null) closeStatement();
			stmt = conn.createStatement();
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return stmt;
	}
	
	public Statement createNewStatementWithoutClosing() throws VerdictException {
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return stmt;
	}
	
	public Statement createStatementIfNotExists() throws VerdictException {
		if (stmt == null) createStatement();
		return stmt;
	}
	
	public void closeStatement() throws VerdictException {
		try {
			if (stmt != null) stmt.close();
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}

	@Override
	public ResultSet getResultSet() {
		return rs;
	}

	@Override
	public DataFrame getDataFrame() {
		return null;
	}

	@Override
	public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues) throws VerdictException {
		StringBuilder sql = new StringBuilder(1000);
		sql.append(String.format("delete from %s ", tableName));
		if (colAndValues.size() > 0) {
			sql.append("where ");
			List<String> conds = new ArrayList<String>();
			for (Pair<String, String> p : colAndValues) {
				conds.add(String.format("%s = %s", p.getLeft(), p.getRight()));
			}
			sql.append(Joiner.on(" AND ").join(conds));
		}
		executeUpdate(sql.toString());
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
	public boolean isJDBC() {
		return true;
	}

	public void close() throws VerdictException {
		try {
			closeStatement();
			if (conn != null) conn.close();
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
}
