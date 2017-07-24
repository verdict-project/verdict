package edu.umich.verdict.dbms;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsMySQL extends DbmsJDBC {

	public DbmsMySQL(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
	}

	/**
	 * Creates a sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	protected void justCreateUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
		String sql = String.format("CREATE TABLE %s AS ", param.sampleTableName()) + 
				 SingleRelation.from(vc, param.originalTable)
			 	 .where("rand() <= " + param.samplingRatio)
				 .select("*, round(rand()*100)%100 AS " + partitionColumnName()).toSql();
		
		VerdictLogger.debug(this, String.format("Creates a table: %s", sql));
		this.executeUpdate(sql);
		VerdictLogger.debug(this, "Done.");
	}
	
	public String modOfHash(String col, int mod) {
		return String.format("mod(cast(conv(substr(md5(%s),17,32),16,10) as unsigned), %d)", col, mod);
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
	
	@Override
	public String modOfRand(int mod) {
		return String.format("abs(rand()) %% %d", mod);
	}

	@Override
	protected String randomPartitionColumn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String randomNumberExpression(SampleParam param) {
		// TODO Auto-generated method stub
		return null;
	}
	
}
