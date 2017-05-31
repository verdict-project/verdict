package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsImpala extends Dbms {

	public DbmsImpala(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
		// TODO Auto-generated constructor stub
	}

	// @return temp table name
	protected TableUniqueName createTempTableWithRand(String originalTableName) throws VerdictException {
		TableUniqueName tempTableName = generateTempTableName();
		VerdictLogger.debug(this, "Creating a temp table with random numbers: " + tempTableName);
		executeUpdate(String.format("CREATE TABLE %s AS SELECT *, rand(unix_timestamp()) as verdict_rand FROM %s",
				tempTableName, originalTableName));
		return tempTableName;
	}
	
	protected void createSampleTableFromTempTable(TableUniqueName tempTableName, TableUniqueName sampleTableName, double sampleRatio)
			throws VerdictException {
		VerdictLogger.debug(this, "Creating a sample table of " + tempTableName);
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE verdict_rand < %f",
				sampleTableName.fullyQuantifiedName(), tempTableName, sampleRatio));
	}

	@Override
	public Pair<Long, Long> createSampleTableOf(String originalTableName, double sampleRatio) throws VerdictException {
		TableUniqueName sampleTableName = vc.getMeta().sampleTableUniqueNameOf(originalTableName);
		TableUniqueName fullyQuantifiedOriginalTableName = TableUniqueName.uname(vc, originalTableName);
		
		dropTable(sampleTableName);
		TableUniqueName tempTableName = createTempTableWithRand(originalTableName);
		createSampleTableFromTempTable(tempTableName, sampleTableName, sampleRatio);
		dropTable(tempTableName);
		
		return Pair.of(getTableSize(sampleTableName), getTableSize(fullyQuantifiedOriginalTableName));
	}
	
	protected TableUniqueName createTempTableExlucdingNameEntry(
			String originalSchemaName, String originalTableName, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = generateTempTableName();
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE originalschemaname <> \"%s\" OR originaltablename <> \"%s\"",
				tempTableName, metaNameTableName, originalSchemaName, originalTableName));
		return tempTableName;
	}
	
	@Override
	public void updateSampleNameEntryIntoDBMS(String originalSchemaName, String originalTableName,
			String sampleSchemaName, String sampleTableName, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingNameEntry(originalSchemaName, originalTableName, metaNameTableName);
		insertSampleNameEntryIntoDBMS(originalSchemaName, originalTableName, sampleSchemaName, sampleTableName, tempTableName);
		VerdictLogger.debug(this, "Created a temp table with the new smaple name info: " + tempTableName);
		
		// copy temp table to the original meta name table after inserting a new entry.
		dropTable(metaNameTableName);
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s", metaNameTableName, tempTableName));
		VerdictLogger.debug(this, String.format("Moved the temp table (%s) to the meta name table (%s).", tempTableName, metaNameTableName));
		dropTable(tempTableName);
	}
	
	protected TableUniqueName createTempTableExlucdingSizeEntry(
			String schemaName, String tableName, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = generateTempTableName();
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE schemaname <> \"%s\" OR tablename <> \"%s\" ",
				tempTableName, metaSizeTableName, schemaName, tableName));
		return tempTableName;
	}
	
	@Override
	public void updateSampleSizeEntryIntoDBMS(String schemaName, String tableName,
			long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingSizeEntry(schemaName, tableName, metaSizeTableName);
		insertSampleSizeEntryIntoDBMS(schemaName, tableName, sampleSize, originalTableSize, tempTableName);
		VerdictLogger.debug(this, "Created a temp table with the new sample size info: " + tempTableName);
		
		// copy temp table to the original meta size table after inserting a new entry.
		dropTable(metaSizeTableName);
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s", metaSizeTableName, tempTableName));
		VerdictLogger.debug(this, String.format("Moved the temp table (%s) to the meta name table (%s).", tempTableName, metaSizeTableName));
		dropTable(tempTableName);
	}
	
	@Override
	public ResultSet getDatabaseNames() throws VerdictException {
		try {
			ResultSet rs = conn.getMetaData().getSchemas(null, "%");
			Map<Integer, Integer> colMap = new HashMap<Integer, Integer>();
			colMap.put(1, 1);
			return new VerdictResultSet(rs, null, colMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	@Override
	public ResultSet getTableNames(String schemaName) throws VerdictException {
		String[] types = {"TABLE"};
		ResultSet rs;
		try {
			rs = conn.getMetaData().getTables(null, schemaName, "%", types);
			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
			columnMap.put(1, 3);	// table name
			return new VerdictResultSet(rs, null, columnMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	@Override
	public ResultSet describeTable(TableUniqueName tableUniqueName)  throws VerdictException {
		try {
			ResultSet rs = conn.getMetaData().getColumns(
					null, tableUniqueName.schemaName, tableUniqueName.tableName, "%");
			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
			columnMap.put(1, 4);	// column name
			columnMap.put(2, 6); 	// data type name
			columnMap.put(3, 12); 	// remarks
			return new VerdictResultSet(rs, null, columnMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	/**
	 * Impala does not support the standard JDBC protocol {@link java.sql.Connection#setCatalog(String) setCatalog}
	 * function for changing the current database. This is a workaround.
	 */
	@Override
	public void changeDatabase(String schemaName) throws VerdictException {
		execute(String.format("use %s", schemaName));
		currentSchema = Optional.fromNullable(schemaName);
		VerdictLogger.info("Database changed to: " + schemaName);
	}
	
	@Override
	public void createMetaTablesInDMBS(
			TableUniqueName originalTableName,
			TableUniqueName sizeTableName,
			TableUniqueName nameTableName) throws VerdictException {
		VerdictLogger.debug(this, "Creating meta tables if not exist.");
		String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName)
				+ " (schemaname STRING, "
				+ " tablename STRING, "
				+ " samplesize BIGINT, "
				+ " originaltablesize BIGINT)";
		executeUpdate(sql);

		sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName)
				+ " (originalschemaname STRING, "
				+ " originaltablename STRING, "
				+ " sampleschemaaname STRING, "
				+ " sampletablename STRING)";
		executeUpdate(sql);
	}
	
	@Override
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
}
