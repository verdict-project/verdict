package edu.umich.verdict;

import java.sql.ResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.VerdictLogger;

import static edu.umich.verdict.util.NameHelpers.*;

public class VerdictMeta {

	// key: fully quantified sample table name
	// value: sample size and the original table size associated with the sample
//	private HashMap<TableUniqueName, SampleInfo> sampleSizeMeta;
	
	// key: fully quantified original table name
	// value: fully quantified sample table name (which should be in META_SCHEMA)
//	private HashMap<TableUniqueName, TableUniqueName> sampleNameMeta;
	
//	private final TableUniqueName META_SIZE_TABLE = TableUniqueName.uname("verdictmeta", "meta_size");
//	private final TableUniqueName META_NAME_TABLE = TableUniqueName.uname("verdictmeta", "meta_name");
	
	private final String META_SIZE_TABLE;
	private final String META_NAME_TABLE;
	
	protected VerdictContext vc;
	
	public VerdictMeta(VerdictContext vc) throws VerdictException {
		this.vc = vc;
		META_NAME_TABLE = vc.getConf().get("meta_name_table");
		META_SIZE_TABLE = vc.getConf().get("meta_size_table");
		
//		replaceMetaInfo();
	}
	
	private Dbms getMetaDbms() {
		return vc.getMetaDbms();
	}
	
//	public void replaceMetaInfo() throws VerdictException {
//		sampleSizeMeta = new HashMap<TableUniqueName, SampleInfo>();
//		sampleNameMeta = new HashMap<TableUniqueName, TableUniqueName>();
//		createMetaTablesInDMBS();	// create if not exists
//		initializeSampleMetaFromDBMS();
//	}
	
//	private void initializeSampleMetaFromDBMS() throws VerdictException {
//		ResultSet rs;
//		try {
//			// sample sizes
//			String sql = String.format("SELECT schemaname, tablename, samplesize, originaltablesize FROM %s",
//					getMetaSizeTableName());
//			rs = getMetaDbms().executeQuery(sql);
//			
//			while (rs.next()) {
//				String schemaName = rs.getString(1);
//				String tableName = rs.getString(2);
//				long sampleSize = rs.getLong(3);
//				long originalTableSize = rs.getLong(4);
//				insertSampleSizeInfoLocally(schemaName, tableName, sampleSize, originalTableSize);
//			}
//			
//			// sample names
//			sql = String.format("SELECT originalschemaname, originaltablename, sampleschemaaname, sampletablename FROM %s",
//					getMetaNameTableName());
//			rs = getMetaDbms().executeQuery(sql);
//			
//			while (rs.next()) {
//				String originalSchemaName = rs.getString(1);
//				String originalTableName = rs.getString(2);
//				String sampleSchemaName = rs.getString(3);
//				String sampleTableName = rs.getString(4);
//				insertSampleNameInfoLocally(originalSchemaName, originalTableName, sampleSchemaName, sampleTableName);
//			}
//		} catch (SQLException e) {
//			throw new VerdictException(StackTraceReader.stackTrace2String(e));
//		}
//	}
	
	/**
	 * Insert sample info into local data structure (for quick access) and into the DBMS (for persistence).
	 * @param originalSchemaName
	 * @param originalTableName
	 * @param sampleSize
	 * @param originalTableSize
	 * @throws VerdictException 
	 */
	public void insertSampleInfo(String originalSchemaName, String originalTableName, long sampleSize, long originalTableSize)
			throws VerdictException {
		TableUniqueName fullSampleName = sampleTableUniqueNameOf(TableUniqueName.uname(originalSchemaName, originalTableName));
		vc.getMetaDbms().createMetaTablesInDMBS(TableUniqueName.uname(originalSchemaName, originalTableName),
				getMetaSizeTableName(fullSampleName),
				getMetaNameTableName(fullSampleName));
		
		getMetaDbms().updateSampleNameEntryIntoDBMS(
				originalSchemaName, originalTableName, fullSampleName.schemaName, fullSampleName.tableName,
				getMetaNameTableName(fullSampleName));
		
		getMetaDbms().updateSampleSizeEntryIntoDBMS(
				fullSampleName.schemaName, fullSampleName.tableName, sampleSize, originalTableSize,
				getMetaSizeTableName(fullSampleName));
		
//		insertSampleNameInfo(originalSchemaName, originalTableName, fullSampleName.schemaName, fullSampleName.tableName);
//		insertSampleSizeInfo(fullSampleName.schemaName, fullSampleName.tableName, sampleSize, originalTableSize);
	}
	
//	private void insertSampleNameInfo(String originalSchemaName, String originalTableName, String sampleSchemaName,
//			String sampleTableName) throws VerdictException {
//		insertSampleNameInfoLocally(originalSchemaName, originalTableName, sampleSchemaName, sampleTableName);
//		
//	}
	
//	private void insertSampleNameInfoLocally(String originalSchemaName, String originalTableName, String sampleSchemaName,
//			String sampleTableName) {
//		TableUniqueName originalTable = new TableUniqueName(originalSchemaName, originalTableName);
//		TableUniqueName sampleTable = new TableUniqueName(sampleSchemaName, sampleTableName);
//		sampleNameMeta.put(originalTable, sampleTable);
//	}
	

	/**
	 * Inserts the size info into {@link #META_SIZE_TABLE} and into the DBMS (for persistence).
	 * @param schemaName sample's schema name
	 * @param tableName sample table name
	 * @param sampleSize
	 * @param originalTableSize
	 * @throws VerdictException 
	 */
//	private void insertSampleSizeInfo(String schemaName, String tableName, long sampleSize, long originalTableSize)
//			throws VerdictException {		
//		insertSampleSizeInfoLocally(schemaName, tableName, sampleSize, originalTableSize);
//		
//	}
	
//	private void insertSampleSizeInfoLocally(String schemaName, String tableName, long sampleSize, long originalTableSize) {
//		TableUniqueName n = new TableUniqueName(schemaName, tableName);
//		SampleInfo s = new SampleInfo(sampleSize, originalTableSize);
//		sampleSizeMeta.put(n, s);
//	}
	
	/**
	 * Delete sample info from {@link #META_SIZE_TABLE} (for quick access) and from the DBMS (for persistence).
	 * @param originalSchemaName
	 * @param originalTableName
	 * @param sampleSize
	 * @param originalTableSize
	 * @throws VerdictException 
	 */
	public void deleteSampleInfo(TableUniqueName originalTableName) throws VerdictException {	
		getMetaDbms().deleteSampleNameEntryFromDBMS(originalTableName.schemaName, originalTableName.tableName,
				getMetaNameTableName(originalTableName));
		TableUniqueName sampleTableName = sampleTableUniqueNameOf(originalTableName);
		getMetaDbms().deleteSampleSizeEntryFromDBMS(sampleTableName.schemaName, sampleTableName.tableName,
				getMetaSizeTableName(sampleTableName));
		
//		deleteSampleNameInfo(originalTableName);
//		deleteSampleSizeInfo(sampleTableUniqueNameOf(originalTableName));
	}
	
//	private void deleteSampleNameInfo(TableUniqueName originalUname) throws VerdictException {
//		sampleNameMeta.remove(originalUname);
//		getMetaDbms().deleteSampleNameEntryFromDBMS(originalUname.schemaName, originalUname.tableName, getMetaNameTableName(originalUname));
//	}

//	private void deleteSampleSizeInfo(TableUniqueName sampleUniqueName) throws VerdictException {
//		sampleSizeMeta.remove(sampleUniqueName);
//		getMetaDbms().deleteSampleSizeEntryFromDBMS(sampleUniqueName.schemaName, sampleUniqueName.tableName, getMetaSizeTableName(sampleUniqueName));
//	}
	
//	/**
//	 * Returns, if exists, the size of the sample for the specified original table. Otherwise return -1.
//	 * @param originalSchemaName
//	 * @param originalTableName
//	 * @return
//	 */
//	public long getSampleSizeByOriginalTableNameIfExists(String originalSchemaName, String originalTableName) {
//		TableUniqueName sampleUname = sampleNameMeta.get(TableUniqueName.uname(originalSchemaName, originalTableName));
//		SampleInfo info = sampleSizeMeta.get(sampleUname);
//		if (info != null)
//			return info.sampleSize;
//		else
//			return -1;
//	}
	
	public Pair<Long, Long> getSampleAndOriginalTableSizeByOriginalTableNameIfExists(TableUniqueName originalTableName) {
		ResultSet rs;
		Long sampleSize = null;
		Long originalTableSize = null;
		try {
			TableUniqueName sampleName = sampleTableUniqueNameOf(originalTableName);
			String sql = String.format("SELECT schemaname, tablename, samplesize, originaltablesize FROM %s"
					+ " WHERE schemaname = \"%s\" AND tablename = \"%s\"",
					getMetaSizeTableName(originalTableName), sampleName.schemaName, sampleName.tableName);
			rs = getMetaDbms().executeQuery(sql);
			
			while (rs.next()) {
				sampleSize = rs.getLong(3);
				originalTableSize = rs.getLong(4);
			}
		} catch (VerdictException | SQLException e) {}
		
		if (sampleSize != null) {
			return Pair.of(sampleSize, originalTableSize);
		} else {
			return Pair.of(-1L, -1L);
		}
	}
	
	/**
	 * Returns the size of the original table, if a sample has been created for the table. Otherwise return -1.
	 * @param originalSchemaName
	 * @param originalTableName
	 * @return
	 */
//	public long getOriginalTableSizeByOriginalTableNameIfSampleExists(String originalSchemaName, String originalTableName) {
//		if (getSampleSizeByOriginalTableNameIfExists(originalSchemaName, originalTableName) >= 0) {
//			TableUniqueName sampleUname = sampleNameMeta.get(TableUniqueName.uname(originalSchemaName, originalTableName));
//			SampleInfo info = sampleSizeMeta.get(sampleUname);
//			return info.originalTableSize; 
//		}
//		else {
//			return -1;
//		}
//	}
	
//	public long getOriginalTableSizeByOriginalTableNameIfSampleExists(TableUniqueName uniqueName) {
//		return getOriginalTableSizeByOriginalTableNameIfSampleExists(uniqueName.schemaName, uniqueName.tableName);
//	}

	public TableUniqueName getSampleTableNameIfExistsElseOriginal(TableUniqueName originalTableName) {
		String sampleSchemaName = null;
		String sampleTableName = null;
		
		try {
			String sql = String.format("SELECT originalschemaname, originaltablename, sampleschemaaname, sampletablename FROM %s"
					+ " WHERE originalschemaname = \"%s\" AND originaltablename = \"%s\"",
					getMetaNameTableName(originalTableName), originalTableName.schemaName, originalTableName.tableName); 
			ResultSet rs = getMetaDbms().executeQuery(sql);
	
			while (rs.next()) {
				sampleSchemaName = rs.getString(3);
				sampleTableName = rs.getString(4);
			}
		} catch (VerdictException | SQLException e) {} 

		if (sampleSchemaName != null) {
			return TableUniqueName.uname(sampleSchemaName, sampleTableName);
		} else {
			return originalTableName;
		}
	}
	
	/**
	 * Obtains the name of the sample table for the given original table. This function performs a syntactic transformation,
	 * without semantic checks.
	 * @param originalTableName
	 * @return
	 */
	public TableUniqueName sampleTableUniqueNameOf(TableUniqueName originalTableName) {
		String localTableName = String.format("sample_%s_%s", originalTableName.schemaName, originalTableName.tableName);
		return TableUniqueName.uname(originalTableName.schemaName, localTableName);
	}
	
	public TableUniqueName sampleTableUniqueNameOf(String originalTableName) {
		return sampleTableUniqueNameOf(TableUniqueName.uname(vc, originalTableName));
	}
	
	/**
	 * 
	 * @param relatedTableName Either the original table or the sample table.
	 * @return
	 */
	public TableUniqueName getMetaSizeTableName(TableUniqueName relatedTableName) {
		return TableUniqueName.uname(relatedTableName.schemaName, META_SIZE_TABLE);
	}
	
	public TableUniqueName getMetaSizeTableName(String schemaName) {
		return TableUniqueName.uname(schemaName, META_SIZE_TABLE);
	}
	
	/**
	 * 
	 * @param relatedTableName Either the original table or the sample table.
	 * @return
	 */
	public TableUniqueName getMetaNameTableName(TableUniqueName relatedTableName) {
		return TableUniqueName.uname(relatedTableName.schemaName, META_NAME_TABLE);
	}
	
	public TableUniqueName getMetaNameTableName(String schemaName) {
		return TableUniqueName.uname(schemaName, META_NAME_TABLE);
	}
}


class SampleInfo {
	public long sampleSize = 0;
	public long originalTableSize = 0;
	
	public SampleInfo(long sampleSize, long originalTableSize) {
		this.sampleSize = sampleSize;
		this.originalTableSize = originalTableSize;
	}
	
	@Override
	public String toString() {
		return String.format("sample (%d out of %d)", sampleSize, originalTableSize);
	}
}
