package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class DbmsDummy extends Dbms {

	public DbmsDummy(VerdictContext vc) throws VerdictException {
		super(vc, "dummy");
	}
	
	@Override
	public boolean execute(String sql) throws VerdictException {
		return false;
	}

	@Override
	public ResultSet getResultSet() {
		return null;
	}

	@Override
	public DataFrame getDataFrame() {
		return null;
	}

	@Override
	public void executeUpdate(String sql) throws VerdictException {
		
	}

	@Override
	public void changeDatabase(String schemaName) throws VerdictException {
	}

	@Override
	public List<Pair<String, String>> getAllTableAndColumns(String schemaName) throws VerdictException {
		return null;
	}

	@Override
	public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
			throws VerdictException {
	}

	@Override
	public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
	}

	@Override
	public long getTableSize(TableUniqueName tableName) throws VerdictException {
		return 0;
	}

	@Override
	public void createMetaTablesInDMBS(TableUniqueName originalTableName, TableUniqueName sizeTableName,
			TableUniqueName nameTableName) throws VerdictException {
	}

	@Override
	public boolean doesMetaTablesExist(String schemaName) throws VerdictException {
		return false;
	}

	@Override
	protected void justCreateUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
	}

	@Override
	protected void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException {
	}

	@Override
	protected void justCreateStratifiedSampleTableof(SampleParam param) throws VerdictException {
	}

	@Override
	public String modOfHash(String col, int mod) {
		return null;
	}

	@Override
	public void close() throws VerdictException {
	}

	@Override
	public List<String> getTables(String schema) throws VerdictException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getColumns(TableUniqueName table) throws VerdictException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getDatabases() throws VerdictException {
		return new HashSet<String>();
	}
	
}
