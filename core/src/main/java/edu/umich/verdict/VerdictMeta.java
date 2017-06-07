package edu.umich.verdict;

import java.sql.ResultSet;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Optional;

import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StackTraceReader;
import edu.umich.verdict.util.VerdictLogger;

import static edu.umich.verdict.util.NameHelpers.*;

public class VerdictMeta {
	
	/**
	 * Works as a cache for a single query execution.
	 * key: original table
	 * value: sample size info
	 */
	private Map<TableUniqueName, SampleInfo> sampleSizeMeta;
	
	/**
	 * Works as a cache for a single query execution.
	 * key: original table
	 * value: sample table
	 */
	private Map<TableUniqueName, TableUniqueName> sampleNameMeta;
	
	/**
	 * remembers for what query id and schema, we have updated the meta info.
	 */
	private Set<Pair<Long, String>> uptodateSchemas;
	
	
	private final String META_SIZE_TABLE;
	private final String META_NAME_TABLE;
	
	protected VerdictContext vc;
	
	public VerdictMeta(VerdictContext vc) throws VerdictException {
		this.vc = vc;
		META_NAME_TABLE = vc.getConf().get("meta_name_table");
		META_SIZE_TABLE = vc.getConf().get("meta_size_table");
		sampleSizeMeta = new HashMap<TableUniqueName, SampleInfo>();
		sampleNameMeta = new HashMap<TableUniqueName, TableUniqueName>();
		uptodateSchemas = new HashSet<Pair<Long, String>>();
	}
	
	private Dbms getMetaDbms() {
		return vc.getMetaDbms();
	}
	
	public void clearSampleInfo() {
		sampleSizeMeta.clear();
		sampleNameMeta.clear();
	}
	
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
	}
		
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
	}
	
	public void refreshSampleInfoIfNeeded(TableUniqueName originalTableName) {
		if (!uptodateSchemas.contains(Pair.of(vc.getCurrentQid(), originalTableName.schemaName))) {
			refreshSampleInfo(originalTableName);
			uptodateSchemas.add(Pair.of(vc.getCurrentQid(), originalTableName.schemaName));
		}
	}
	
	public void refreshSampleInfo(TableUniqueName originalTableName) {
		ResultSet rs;
		
		TableUniqueName metaNameTable = getMetaNameTableName(originalTableName);
		TableUniqueName metaSizeTable = getMetaSizeTableName(originalTableName);
		
		try {
			String sql = String.format("SELECT originalschemaname, originaltablename, "
					+ "sampleschemaaname, sampletablename FROM %s", metaNameTable);
			rs = getMetaDbms().executeQuery(sql);
			
			while (rs.next()) {
				String originalSchemaName = rs.getString(1);
				String originalTabName = rs.getString(2);
				String sampleSchemaName = rs.getString(3);
				String sampleTabName = rs.getString(4);
				sampleNameMeta.put(TableUniqueName.uname(originalSchemaName, originalTabName),
						TableUniqueName.uname(sampleSchemaName, sampleTabName));
			}
			
			sql = String.format("SELECT schemaaname, tablename, samplesize, originaltablesize "
					+ " FROM %s", metaSizeTable);
			rs = getMetaDbms().executeQuery(sql);
			
			while (rs.next()) {
				String sampleSchemaName = rs.getString(1);
				String sampleTabName = rs.getString(2);
				Long sampleSize = rs.getLong(3);
				Long originalTableSize = rs.getLong(4);
				sampleSizeMeta.put(TableUniqueName.uname(sampleSchemaName, sampleTabName),
						new SampleInfo(sampleSize, originalTableSize));
			}
			
//			String sql = String.format("SELECT originalschemaname, originaltablename, "
//					+ "sampleschemaaname, sampletablename, "
//					+ "samplesize, originaltablesize FROM %s, %s "
//					+ "WHERE %s.sampleschemaaname = %s.schemaname AND %s.sampletablename = %s.tablename",
//					metaNameTable, metaSizeTable,
//					metaNameTable, metaSizeTable, metaNameTable, metaSizeTable);
//			rs = getMetaDbms().executeQuery(sql);
//
//			while (rs.next()) {
//				String originalSchemaName = rs.getString(1);
//				String originalTabName = rs.getString(2);
//				String sampleSchemaName = rs.getString(3);
//				String sampleTabName = rs.getString(4);
//				Long sampleSize = rs.getLong(5);
//				Long originalTableSize = rs.getLong(6);
//				
//				sampleNameMeta.put(TableUniqueName.uname(originalSchemaName, originalTabName),
//						TableUniqueName.uname(sampleSchemaName, sampleTabName));
//				sampleSizeMeta.put(TableUniqueName.uname(originalSchemaName, originalTabName),
//						new SampleInfo(sampleSize, originalTableSize));
//			}
		} catch (VerdictException | SQLException e) {}

		VerdictLogger.debug(this, "Sample meta data updated.");
	}

	public Pair<Long, Long> getSampleAndOriginalTableSizeByOriginalTableNameIfExists(TableUniqueName originalTableName) {
		refreshSampleInfoIfNeeded(originalTableName);
		
		if (sampleSizeMeta.containsKey(originalTableName)) {
			SampleInfo info = sampleSizeMeta.get(originalTableName);
			return Pair.of(info.sampleSize, info.originalTableSize);
		} else {
			return Pair.of(-1L, -1L);
		}
	}

	public TableUniqueName getSampleTableNameIfExistsElseOriginal(TableUniqueName originalTableName) {
		refreshSampleInfoIfNeeded(originalTableName);
		
		if (sampleNameMeta.containsKey(originalTableName)) {
			return sampleNameMeta.get(originalTableName);
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
