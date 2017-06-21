package edu.umich.verdict.dbms;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.VerdictLogger;

import static edu.umich.verdict.util.NameHelpers.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;

/**
 * This class is responsible for choosing a right DBMS class.
 */
public class Dbms {
	
	protected final Connection conn;
	protected final String dbName;
	protected Optional<String> currentSchema;
	protected VerdictContext vc;
	private Statement stmt;		// created Statements must be registered here.
	
	/**
	 * Copy constructor for not sharing the underlying statement.
	 * @param another
	 */
	public Dbms(Dbms another) {
		conn = another.conn;
		dbName = another.dbName;
		currentSchema = another.currentSchema;
		vc = another.vc;
		stmt = null;
		VerdictLogger.debug(this, "A new dbms connection with schema: " + currentSchema);
	}
	
	protected Dbms(VerdictContext vc,
			    String dbName,
			    String host,
			    String port,
			    String schema,
			    String user,
			    String password,
			    String jdbcClassName)
			throws VerdictException {
		this.vc = vc;
		this.dbName = dbName;
		currentSchema = Optional.fromNullable(schema);
		String url = composeUrl(dbName,
							   host,
							   port,
							   schema,
							   user,
							   password);
		conn = makeDbmsConnection(url, jdbcClassName);
	}
	
	public static Dbms getInstance(VerdictContext vc,
			String dbName,
			String host,
			String port,
			String schema,
			String user,
			String password,
			String jdbcClassName) throws VerdictException {
		Dbms dbms = null;
		if (dbName.equals("mysql")) {
			dbms = new DbmsMySQL(vc, dbName, host, port, schema, user, password, jdbcClassName);
		} else if (dbName.equals("impala")) {
			dbms = new DbmsImpala(vc, dbName, host, port, schema, user, password, jdbcClassName);
		} else if (dbName.equals("hive") || dbName.equals("hive2")) {
			dbms = new DbmsHive(vc, dbName, host, port, schema, user, password, jdbcClassName);
		} else {
			String msg = String.format("Unsupported DBMS: %s", dbName);
			VerdictLogger.error("Dbms", msg);
			throw new VerdictException(msg);
		}
		return dbms;
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
	
	protected String composeUrl(String dbms, String host, String port, String schema, String user, String password) throws VerdictException {
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
		
		if (vc.getConf().doesContain("principal")) {
			String principal = vc.getConf().get("principal");
			
			Pattern princPattern = Pattern.compile("(?<service>.*)/(?<host>.*)@(?<realm>.*)");
			
			Matcher princMatcher = princPattern.matcher(principal);
			
			if (princMatcher.find()) {
				String service = princMatcher.group("service");
				String krbRealm = princMatcher.group("realm");
				String krbHost = princMatcher.group("host");
				
				url.append(String.format(";AuthMech=%s;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s;KrbAuthType=%s",
						 "1", krbRealm, krbHost, service, "2"));
			} else {
				VerdictLogger.error("Error: principal \"" + principal + "\" could not be parsed.\n"
						+ "Make sure the principal is in the form service/host@REALM");
			}		
		}
		
		return url.toString();
	}

	public String getName() {
		return dbName;
	}
	
	public Optional<String> getCurrentSchema() {
		return currentSchema;
	}
	
	public boolean execute(String query) throws VerdictException {
		createStatementIfNotExists();
		try {
			return stmt.execute(query);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public ResultSet executeQuery(String query) throws VerdictException {
		createStatementIfNotExists();
		ResultSet rs;
		try {
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return rs;
	}
	
	public void executeUpdate(String query) throws VerdictException { 
		createStatementIfNotExists();
		try {
			stmt.executeUpdate(query);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
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
	
	/**
	 * changes to another database (or equivalently, schema). This is conceptually equal to the use statement in MySQL.
	 * @throws VerdictException
	 */
	public void changeDatabase(String schemaName) throws VerdictException {
		try {
			conn.setCatalog(schemaName);
			currentSchema = Optional.fromNullable(schemaName);
			VerdictLogger.info("Database changed to: " + schemaName);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public ResultSet showTables() throws VerdictException {
		try {
			DatabaseMetaData md = conn.getMetaData();
			ResultSet rs = md.getTables(null, null, "%", null);
			return rs;
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public void dropTable(TableUniqueName tableName) throws VerdictException {
		String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
		VerdictLogger.debug(this, String.format("Drop table: %s", sql));
		this.executeUpdate(sql);
	}
	
	/**
	 * Creates a sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	protected void justCreateSampleTableOf(TableUniqueName originalTableName, double sampleRatio) throws VerdictException {
		TableUniqueName sampleTableName = vc.getMeta().sampleTableUniqueNameOf(originalTableName);
		String sql = String.format("CREATE TABLE %s SELECT * FROM %s WHERE rand() < %f;",
										sampleTableName.fullyQuantifiedName(), originalTableName, sampleRatio);
		VerdictLogger.debug(this, String.format("Create a table: %s", sql));
		this.executeUpdate(sql);
	}
	
	public long getTableSize(TableUniqueName tableName) throws VerdictException {
		ResultSet rs;
		long cnt = 0;
		try {
			String sql = String.format("SELECT COUNT(*) FROM %s", tableName);
			rs = this.executeQuery(sql);
			while(rs.next()) {cnt = rs.getLong(1);	}
			rs.close();
		} catch (SQLException e) {
			throw new VerdictException(StackTraceReader.stackTrace2String(e));
		}
		return cnt;
	}

	public Pair<Long, Long> createSampleTableOf(String originalTableName, double sampleRatio) throws VerdictException {
		TableUniqueName sampleTableName = vc.getMeta().sampleTableUniqueNameOf(originalTableName);
		TableUniqueName fullyQuantifiedOriginalTableName = TableUniqueName.uname(vc, originalTableName);

		dropTable(sampleTableName);
		justCreateSampleTableOf(fullyQuantifiedOriginalTableName, sampleRatio);
		
		return Pair.of(getTableSize(sampleTableName), getTableSize(fullyQuantifiedOriginalTableName));
	}

	public Connection getDbmsConnection() {
		return conn;
	}

	public void close() throws VerdictException {
		try {
			closeStatement();
			if (conn != null) conn.close();
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public void updateSampleNameEntryIntoDBMS(String originalSchemaName, String originalTableName,
			String sampleSchemaName, String sampleTableName, TableUniqueName metaNameTableName) throws VerdictException {
		deleteSampleNameEntryFromDBMS(originalSchemaName, originalTableName, metaNameTableName);
		insertSampleNameEntryIntoDBMS(originalSchemaName, originalTableName, sampleSchemaName, sampleTableName, metaNameTableName);
	}
	
	public void deleteSampleNameEntryFromDBMS(String originalSchemaName, String originalTableName, TableUniqueName metaNameTableName)
			throws VerdictException {
		String sql = String.format("DELETE FROM %s WHERE originalschemaname = \"%s\" AND originaltablename = \"%s\"",
				metaNameTableName, originalSchemaName, originalTableName);
		executeUpdate(sql);
	}
	
	protected void insertSampleNameEntryIntoDBMS(String originalSchemaName, String originalTableName,
			String sampleSchemaName, String sampleTableName, TableUniqueName metaNameTableName) throws VerdictException {
		String sql = String.format("INSERT INTO %s VALUES (\"%s\", \"%s\", \"%s\", \"%s\")", metaNameTableName,
				originalSchemaName, originalTableName, sampleSchemaName, sampleTableName);
		executeUpdate(sql);
	}
	
	public void updateSampleSizeEntryIntoDBMS(String schemaName, String tableName,
			long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		deleteSampleSizeEntryFromDBMS(schemaName, tableName, metaSizeTableName);
		insertSampleSizeEntryIntoDBMS(schemaName, tableName, sampleSize, originalTableSize, metaSizeTableName);
	}
	
	public void deleteSampleSizeEntryFromDBMS(String schemaName, String tableName, TableUniqueName metaSizeTableName) throws VerdictException {
		String sql = String.format("DELETE FROM %s WHERE schemaname = \"%s\" AND tablename = \"%s\" ",
				metaSizeTableName, schemaName, tableName);
		executeUpdate(sql);
	}
	
	protected void insertSampleSizeEntryIntoDBMS(String schemaName, String tableName,
			long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		String sql = String.format("INSERT INTO %s VALUES (\"%s\", \"%s\", %d, %d)", metaSizeTableName, schemaName, tableName, sampleSize, originalTableSize);
		executeUpdate(sql);
	}
	
	/**
	 * This method does not guarantee fast deletion (especially in Impala or Hive)
	 * @param tableName
	 * @param condition
	 * @throws VerdictException 
	 */
	public void deleteRowsIn(String tableName, String condition) throws VerdictException {
		String sql = String.format("DELETE FROM %s WHERE %s", tableName, condition);
		this.executeUpdate(sql);
	}
	
	/**
	 * This method does not guarantee fast insertion (especially in Impala or Hive)
	 * @param tableName
	 * @param condition
	 * @throws VerdictException 
	 */
	public void insertRowsIn(String tableName, String values) throws VerdictException {
		String sql = String.format("INSERT INTO %s VALUES (%s)", tableName, values);
		this.executeUpdate(sql);
	}
	
	/**
	 * This method is not thread-safe
	 * @return
	 */
	public TableUniqueName generateTempTableName() {
		String tableName = String.format("verdict_temp_table_%d", System.nanoTime());
		return TableUniqueName.uname(vc, tableName);
	}
	
	public ResultSet getDatabaseNames() throws VerdictException {
		try {
			return conn.getMetaData().getCatalogs();
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public ResultSet getTableNames(String schemaName) throws VerdictException {
		try {
			String[] types = {"TABLE"};
			ResultSet rs = conn.getMetaData().getTables(schemaName, null, "%", types);
			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
			columnMap.put(1, 3);	// table name
			return new VerdictResultSet(rs, null, columnMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public ResultSet describeTable(TableUniqueName tableUniqueName)  throws VerdictException {
		try {
			ResultSet rs = conn.getMetaData().getColumns(
					tableUniqueName.schemaName, null, tableUniqueName.tableName, "%");
			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
			columnMap.put(1, 4);	// column name
			columnMap.put(2, 6); 	// data type name
			columnMap.put(3, 12); 	// remarks
			return new VerdictResultSet(rs, null, columnMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public void createMetaTablesInDMBS(
			TableUniqueName originalTableName,
			TableUniqueName sizeTableName,
			TableUniqueName nameTableName) throws VerdictException {
		VerdictLogger.debug(this, "Creating meta tables if not exist.");
		String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName)
				+ " (schemaname VARCHAR(50), "
				+ " tablename VARCHAR(50), "
				+ " samplesize BIGINT, "
				+ " originaltablesize BIGINT)";
		executeUpdate(sql);

		sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName)
				+ " (originalschemaname VARCHAR(50), "
				+ " originaltablename VARCHAR(50), "
				+ " sampleschemaaname VARCHAR(50), "
				+ " sampletablename VARCHAR(50))";
		executeUpdate(sql);
	}
	
	public boolean doesMetaTablesExist(String schemaName) throws VerdictException {
		String[] types = {"TABLE"};
		try {
			ResultSet rs = vc.getDbms().getDbmsConnection().getMetaData().getTables(
					null, schemaName, vc.getMeta().getMetaNameTableName(currentSchema.get()).tableName, types);
			if (!rs.next()) return false;
			else return true;
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	public String getQuoteString() {
		return "\"";
	}
}
