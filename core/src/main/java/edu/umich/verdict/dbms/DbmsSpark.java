package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsSpark extends Dbms {
	
	private static String DBNAME = "spark";

	protected SQLContext sqlContext;
	
	protected DataFrame df;
	
	protected Set<TableUniqueName> cachedTable;

	public DbmsSpark(VerdictContext vc, SQLContext sqlContext) throws VerdictException {	
		super(vc, DBNAME);

		this.sqlContext = sqlContext;
		this.cachedTable = new HashSet<TableUniqueName>();
	}

	public DataFrame getDatabaseNamesInDataFrame() throws VerdictException {
		DataFrame df = executeSparkQuery("show databases");
		return df;
	}

	public DataFrame getTablesInDataFrame(String schemaName) throws VerdictException {
		DataFrame df = executeSparkQuery("show tables in " + schemaName);
		return df;
	}
	
	public DataFrame describeTableInDataFrame(TableUniqueName tableUniqueName)  throws VerdictException {
		DataFrame df = executeSparkQuery(String.format("describe %s", tableUniqueName));
		return df;
	}

	@Override
	public boolean execute(String sql) throws VerdictException {
		df = sqlContext.sql(sql);
        return (df != null)? true : false;
		//return (df.count() > 0)? true : false;
	}

	@Override
	public void executeUpdate(String sql) throws VerdictException {
		execute(sql);
	}

	@Override
	public ResultSet getResultSet() {
		return null;
	}

	@Override
	public DataFrame getDataFrame() {
		return df;
	}
	
	public DataFrame emptyDataFrame() {
		return sqlContext.emptyDataFrame();
	}

	@Override
	public Set<String> getDatabases() throws VerdictException {
		Set<String> databases = new HashSet<String>();
		List<Row> rows = getDatabaseNamesInDataFrame().collectAsList();
		for (Row row : rows) {
			String dbname = row.getString(0); 
			databases.add(dbname);
		}
		return databases;
	}

	@Override
	public List<String> getTables(String schema) throws VerdictException {
		List<String> tables = new ArrayList<String>();
		List<Row> rows = getTablesInDataFrame(schema).collectAsList();
		for (Row row : rows) {
			String table = row.getString(0);
			tables.add(table);
		}
		return tables;
	}

	@Override
	public long getTableSize(TableUniqueName tableName) throws VerdictException {
		String sql = String.format("select count(*) from %s", tableName);
		DataFrame df = executeSparkQuery(sql);
		long size = df.collectAsList().get(0).getLong(0);
		return size;
	}

	@Override
	public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
		Map<String, String> col2type = new LinkedHashMap<String, String>();
		List<Row> rows = describeTableInDataFrame(table).collectAsList();
		for (Row row : rows) {
			String column = row.getString(0);
			String type = row.getString(1);
			col2type.put(column, type);
		}
		return col2type;
	}

	@Override
	public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
			throws VerdictException {
		VerdictLogger.warn(this, "deleteEntry() not implemented for DbmsSpark");
	}

	@Override
	public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
		StringBuilder sql = new StringBuilder(1000);
		sql.append(String.format("insert into %s ", tableName));
		sql.append("select t.* from (select ");
		String with = "'";
		sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
		sql.append(") t");
		executeUpdate(sql.toString());
	}
	
	@Override
	public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingNameEntry(param, metaNameTableName);
		insertSampleNameEntryIntoDBMS(param, tempTableName);
		moveTable(tempTableName, metaNameTableName);
	}
	
	protected TableUniqueName createTempTableExlucdingNameEntry(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		String metaSchema = param.sampleTableName().getSchemaName();
		TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
		TableUniqueName originalTableName = param.originalTable;
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s "
				+ "WHERE originalschemaname <> \"%s\" OR originaltablename <> \"%s\" OR sampletype <> \"%s\""
				+ "OR samplingratio <> %s OR columnnames <> \"%s\"",
				tempTableName, metaNameTableName, originalTableName.getSchemaName(), originalTableName.getTableName(),
				param.sampleType, samplingRatioToString(param.samplingRatio), columnNameListToString(param.columnNames)));
		return tempTableName;
	}
	
	@Override
	public void updateSampleSizeEntryIntoDBMS(SampleParam param, long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
		insertSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, tempTableName);
		moveTable(tempTableName, metaSizeTableName);
	}

	protected TableUniqueName createTempTableExlucdingSizeEntry(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		String metaSchema = param.sampleTableName().getSchemaName();
		TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
		TableUniqueName sampleTableName = param.sampleTableName();
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE schemaname <> \"%s\" OR tablename <> \"%s\" ",
				tempTableName, metaSizeTableName, sampleTableName.getSchemaName(), sampleTableName.getTableName()));
		return tempTableName;
	}
	
	@Override
	public void deleteSampleNameEntryFromDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTable = createTempTableExlucdingNameEntry(param, metaNameTableName);
		moveTable(tempTable, metaNameTableName);
	}
	
	@Override
	public void deleteSampleSizeEntryFromDBMS(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTable = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
		moveTable(tempTable, metaSizeTableName);
	}
	
	@Override
	public void cacheTable(TableUniqueName tableName) {
		if (vc.getConf().cacheSparkSamples() && !cachedTable.contains(tableName)) {
			sqlContext.cacheTable(tableName.toString());
			cachedTable.add(tableName);
		}
	}

	@Override
	public String modOfHash(String col, int mod) {
		return String.format("crc32(cast(%s as string)) %% %d", col, mod);
	}
	
	@Override
	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
	}

	@Override
	protected String randomNumberExpression(SampleParam param) {
		String expr = "rand(unix_timestamp())";
		return expr;
	}

	@Override
	public boolean isSpark() {
		return true;
	}

	@Override
	public void close() throws VerdictException {
		// TODO Auto-generated method stub
	}
	
	@Override
	protected String modOfRand(int mod) {
		return String.format("pmod(abs(rand(unix_timestamp())), %d)", mod);
	}
	
}
