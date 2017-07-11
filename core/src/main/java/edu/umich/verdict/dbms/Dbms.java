package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.Sets;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

/**
 * This class is responsible for choosing a right DBMS class.
 */
public abstract class Dbms {
	
	protected final String dbName;
	
	protected Optional<String> currentSchema;
	
	protected VerdictContext vc;
	
	
	public VerdictContext getVc() {
		return vc;
	}

	public void setVc(VerdictContext vc) {
		this.vc = vc;
	}

	public String getDbName() {
		return dbName;
	}

	public void setCurrentSchema(Optional<String> currentSchema) {
		this.currentSchema = currentSchema;
	}

	/**
	 * Copy constructor for not sharing the underlying statement.
	 * @param another
	 */
	public Dbms(Dbms another) {
		dbName = another.dbName;
		currentSchema = another.currentSchema;
		vc = another.vc;
	}
	
	protected Dbms(VerdictContext vc, String dbName) {
		this.vc = vc;
		this.dbName = dbName;
		currentSchema = Optional.absent();
	}
	
	public static Dbms from(VerdictContext vc, VerdictConf conf) throws VerdictException {
		Dbms dbms = Dbms.getInstance(
			 	  vc,
			 	  conf.getDbms(),
			 	  conf.getHost(),
			 	  conf.getPort(),
			 	  conf.getDbmsSchema(),
			 	  (conf.getBoolean("no_user_password"))? "" : conf.getUser(),
			 	  (conf.getBoolean("no_user_password"))? "" : conf.getPassword(),
			 	  conf.get(conf.getDbms() + ".jdbc_class_name"));
		
		Set<String> jdbcDbmsNames = Sets.newHashSet("mysql", "impala", "hive", "hive2");
		
		if (jdbcDbmsNames.contains(conf.getDbms())) {
			VerdictLogger.info(
					(conf.getDbmsSchema() != null) ?
							String.format("Connected to database: %s//%s:%s/%s",
									conf.getDbms(), conf.getHost(), conf.getPort(), conf.getDbmsSchema())
							: String.format("Connected to database: %s//%s:%s",
									conf.getDbms(), conf.getHost(), conf.getPort()));
		}
		
		return dbms;
	}
	
	protected static Dbms getInstance(VerdictContext vc,
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
		} else if (dbName.equals("dummy")) {
			dbms = new DbmsDummy(vc);
		} else {
			String msg = String.format("Unsupported DBMS: %s", dbName);
			VerdictLogger.error("Dbms", msg);
			throw new VerdictException(msg);
		}
		
		return dbms;
	}

	public String getName() {
		return dbName;
	}
	
	public Optional<String> getCurrentSchema() {
		return currentSchema;
	}
	
	public ResultSet executeJdbcQuery(String sql) throws VerdictException {
		execute(sql);
		ResultSet rs = getResultSet();
		return rs;
	}
	
	public DataFrame executeSparkQuery(String sql) throws VerdictException {
		execute(sql);
		DataFrame rs = getDataFrame();
		return rs;
	}
	
	public abstract boolean execute(String sql) throws VerdictException;
	
	public abstract ResultSet getResultSet();
	
	public abstract DataFrame getDataFrame();
	
	public abstract void executeUpdate(String sql) throws VerdictException;

	public abstract void changeDatabase(String schemaName) throws VerdictException;
	
	public void createDatabase(String database) throws VerdictException {
		createCatalog(database);
	}
	
	public void createCatalog(String catalog) throws VerdictException {
		String sql = String.format("create database if not exists %s", catalog);
		executeUpdate(sql);
	}
	
	public void dropTable(TableUniqueName tableName) throws VerdictException {
		Set<String> databases = vc.getMeta().getDatabases();
		if (!databases.contains(tableName.getSchemaName())) {
			VerdictLogger.debug(this, String.format("Database, %s, does not exists. Verdict doesn't bother to run a drop table statement.", tableName.getSchemaName()));
			return;
		}
		
		List<String> tables = getTables(tableName.getSchemaName());
		if (!tables.contains(tableName.getTableName())) {
			VerdictLogger.debug(this, String.format("Table, %s, does not exists. Verdict doesn't bother to run a drop table statement.", tableName));
			return;
		}
		
		String sql = String.format("DROP TABLE IF EXISTS %s", tableName);
		VerdictLogger.debug(this, String.format("Drops table: %s", sql));
		executeUpdate(sql);
		VerdictLogger.debug(this, tableName + " has been dropped.");
	}
	
	public void moveTable(TableUniqueName from, TableUniqueName to) throws VerdictException {
		VerdictLogger.debug(this, String.format("Moves table %s to table %s", from, to));
		String sql = String.format("CREATE TABLE %s AS SELECT * FROM %s", to, from);
		dropTable(to);
		executeUpdate(sql);
		dropTable(from);
		VerdictLogger.debug(this, "Moving table done.");
	}

	public List<Pair<String, String>> getAllTableAndColumns(String schema) throws VerdictException {
		Set<String> databases = vc.getMeta().getDatabases();
		if (!databases.contains(schema)) {
			return Arrays.asList();
		}
		
		List<Pair<String, String>> tablesAndColumns = new ArrayList<Pair<String, String>>();
		List<String> tables = getTables(schema);
		for (String table : tables) {
			List<String> columns = getColumns(TableUniqueName.uname(schema, table));
			for (String column : columns) {
				tablesAndColumns.add(Pair.of(table, column));
			}
		}
		return tablesAndColumns;
	}
	
	public abstract Set<String> getDatabases() throws VerdictException;
	
	public abstract List<String> getTables(String schema) throws VerdictException;
	
	public abstract List<String> getColumns(TableUniqueName table) throws VerdictException;

	public abstract void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues) throws VerdictException;

	public abstract void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException;

	public abstract long getTableSize(TableUniqueName tableName) throws VerdictException;

	public abstract void createMetaTablesInDMBS(TableUniqueName originalTableName,
												TableUniqueName sizeTableName,
												TableUniqueName nameTableName) throws VerdictException;
	
	public boolean doesMetaTablesExist(String schemaName) throws VerdictException {
		String metaSchema = vc.getMeta().metaCatalogForDataCatalog(schemaName);
		String metaNameTable = vc.getMeta().getMetaNameTableForOriginalSchema(schemaName).getTableName();
		String metaSizeTable = vc.getMeta().getMetaSizeTableForOriginalSchema(schemaName).getTableName();
		
		Set<String> tables = new HashSet<String>(getTables(metaSchema));
		if (tables.contains(metaNameTable) && tables.contains(metaSizeTable)) {
			return true;
		} else {
			return false;
		}
	}

	public Pair<Long, Long> createUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
		dropTable(param.sampleTableName());
		justCreateUniformRandomSampleTableOf(param);
		return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
	}

	/**
	 * Creates a sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	protected abstract void justCreateUniformRandomSampleTableOf(SampleParam param) throws VerdictException;
	
	public Pair<Long, Long> createUniverseSampleTableOf(SampleParam param) throws VerdictException {
		dropTable(param.sampleTableName());
		justCreateUniverseSampleTableOf(param);
		return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
	}

	/**
	 * Creates a universe sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	protected abstract void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException;
	
	public Pair<Long, Long> createStratifiedSampleTableOf(SampleParam param) throws VerdictException {
		dropTable(param.sampleTableName());
		justCreateStratifiedSampleTableof(param);
		return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
	}
	
	protected abstract void justCreateStratifiedSampleTableof(SampleParam param) throws VerdictException;

	public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		deleteSampleNameEntryFromDBMS(param, metaNameTableName);
		insertSampleNameEntryIntoDBMS(param, metaNameTableName);
	}
	
	public void deleteSampleNameEntryFromDBMS(SampleParam param, TableUniqueName metaNameTableName)
			throws VerdictException {
		TableUniqueName originalTableName = param.originalTable;
		List<Pair<String, String>> colAndValues = new ArrayList<Pair<String, String>>();
		colAndValues.add(Pair.of("originalschemaname", originalTableName.getSchemaName()));
		colAndValues.add(Pair.of("originaltablename", originalTableName.getTableName()));
		colAndValues.add(Pair.of("sampletype", param.sampleType));
		colAndValues.add(Pair.of("samplingratio", samplingRatioToString(param.samplingRatio)));
		colAndValues.add(Pair.of("columnnames", columnNameListToString(param.columnNames)));
		deleteEntry(metaNameTableName, colAndValues);
	}
	
	protected void insertSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName originalTableName = param.originalTable;
		TableUniqueName sampleTableName = param.sampleTableName();
		
		List<Object> values = new ArrayList<Object>();
		values.add(originalTableName.getSchemaName());
		values.add(originalTableName.getTableName());
		values.add(sampleTableName.getSchemaName());
		values.add(sampleTableName.getTableName());
		values.add(param.sampleType);
		values.add(param.samplingRatio);
		values.add(columnNameListToString(param.columnNames));
		
		insertEntry(metaNameTableName, values);
	}
	
	public void updateSampleSizeEntryIntoDBMS(SampleParam param, long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		deleteSampleSizeEntryFromDBMS(param, metaSizeTableName);
		insertSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, metaSizeTableName);
	}
	
	public void deleteSampleSizeEntryFromDBMS(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName sampleTableName = param.sampleTableName();
		List<Pair<String, String>> colAndValues = new ArrayList<Pair<String, String>>();
		colAndValues.add(Pair.of("schemaname", sampleTableName.getSchemaName()));
		colAndValues.add(Pair.of("tablename", sampleTableName.getTableName()));
		deleteEntry(metaSizeTableName, colAndValues);
	}
	
	protected void insertSampleSizeEntryIntoDBMS(SampleParam param,	long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName sampleTableName = param.sampleTableName();
		List<Object> values = new ArrayList<Object>();
		values.add(sampleTableName.getSchemaName());
		values.add(sampleTableName.getTableName());
		values.add(sampleSize);
		values.add(originalTableSize);
		insertEntry(metaSizeTableName, values);
	}
	
	public void cacheTable(TableUniqueName tableName) {}
	
	@Deprecated
	public String getQuoteString() {
		return "`";
	}

	@Deprecated
	public String varianceFunction() {
		return "VAR_SAMP";
	}
	
	@Deprecated
	public String stddevFunction() {
		return "STDDEV";
	}

	public String partitionColumnName() {
		return vc.getConf().get("verdict.subsampling_partition_column_name");
	}
	
	public int partitionCount() {
		return vc.getConf().getInt("verdict.subsampling_partition_count");
	}

	public String samplingProbabilityColumnName() {
		return vc.getConf().get("verdict.sampling_probability_column");
	}
	
	public abstract String modOfHash(String col, int mod);

	protected String quote(String expr) {
		return String.format("\"%s\"", expr);
	}

	protected String columnNameListToString(List<String> columnNames) {
		return Joiner.on(",").join(columnNames);
	}

	protected String samplingRatioToString(double samplingRatio) {
		return String.format("%.4f", samplingRatio);
	}
	
	public boolean isJDBC() {
		return false;
	}
	
	public boolean isSpark() {
		return false;
	}

	public abstract void close() throws VerdictException;
}
