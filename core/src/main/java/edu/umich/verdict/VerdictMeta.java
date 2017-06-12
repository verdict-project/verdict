package edu.umich.verdict;

import java.sql.ResultSet;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictMeta {
	
	/**
	 * Works as a cache for a single query execution.
	 * key: sample table
	 * value: sample size info
	 */
	private Map<TableUniqueName, SampleSizeInfo> sampleSizeMeta;
	
	/**
	 * Works as a cache for a single query execution.
	 * key: original table
	 * value: key: sample creation params
	 * 	      value: sample table
	 */
	private Map<TableUniqueName, Map<SampleParam, TableUniqueName>> sampleNameMeta;
	
	/**
	 * remembers for what query id and schema, we have updated the meta info.
	 */
	private Set<Pair<Long, String>> uptodateSchemas;
	
	/**
	 * remembers tables and their column names.
	 */
	private Map<TableUniqueName, List<String>> tableToColumnNames;
	
	private final String META_SIZE_TABLE;
	private final String META_NAME_TABLE;
	
	protected VerdictContext vc;
	
	public VerdictMeta(VerdictContext vc) throws VerdictException {
		this.vc = vc;
		META_NAME_TABLE = vc.getConf().get("meta_name_table");
		META_SIZE_TABLE = vc.getConf().get("meta_size_table");
		sampleSizeMeta = new HashMap<TableUniqueName, SampleSizeInfo>();
		sampleNameMeta = new HashMap<TableUniqueName, Map<SampleParam, TableUniqueName>>();
		uptodateSchemas = new HashSet<Pair<Long, String>>();
		tableToColumnNames = new HashMap<TableUniqueName, List<String>>();
	}
	
	private Dbms getMetaDbms() {
		return vc.getMetaDbms();
	}
	
	public void clearSampleInfo() {
		sampleSizeMeta.clear();
		sampleNameMeta.clear();
	}
	
	public List<String> getColumnNames(TableUniqueName tableName) {
		refreshSampleInfoIfNeeded(tableName.schemaName);
		if (tableToColumnNames.containsKey(tableName)) {
			return tableToColumnNames.get(tableName);
		} else {
			return new ArrayList<String>();
		}
	}
	
	public Map<TableUniqueName, List<String>> getTableAndColumnNames(String schemaName) {
		refreshSampleInfoIfNeeded(schemaName);
		return tableToColumnNames;
	}
	
	/**
	 * Insert sample info into local data structure (for quick access) and into the DBMS (for persistence).
	 * @param originalSchemaName
	 * @param originalTableName
	 * @param sampleSize
	 * @param originalTableSize
	 * @throws VerdictException 
	 */
	public void insertSampleInfo(String originalSchemaName, String originalTableName, String sampleTableName,
								 long sampleSize, long originalTableSize,
								 String sampleType, Double samplingRatio, List<String> columnNames) throws VerdictException {
		TableUniqueName fullSampleName = TableUniqueName.uname(originalSchemaName, sampleTableName);
		vc.getMetaDbms().createMetaTablesInDMBS(TableUniqueName.uname(originalSchemaName, originalTableName),
				getMetaSizeTableName(fullSampleName),
				getMetaNameTableName(fullSampleName));
		
		getMetaDbms().updateSampleNameEntryIntoDBMS(
				originalSchemaName, originalTableName, fullSampleName.schemaName, fullSampleName.tableName,
				sampleType, samplingRatio, columnNames,
				getMetaNameTableName(fullSampleName));
		
		getMetaDbms().updateSampleSizeEntryIntoDBMS(
				fullSampleName.schemaName, fullSampleName.tableName, sampleSize, originalTableSize,
				getMetaSizeTableName(fullSampleName));
	}
		
	/**
	 * Delete sample info from {@link #META_SIZE_TABLE} (for quick access) and from the DBMS (for persistence).
	 * @param originalTableName
	 * @throws VerdictException 
	 */
	public void deleteSampleInfo(TableUniqueName originalTableName,
			String sampleType, double samplingRatio, List<String> columnNames) throws VerdictException {
		refreshSampleInfoIfNeeded(originalTableName.schemaName);
		SampleParam p = new SampleParam(originalTableName, sampleType, samplingRatio, columnNames);
		
		if (sampleNameMeta.containsKey(originalTableName)) {
			TableUniqueName sampleTableName = sampleNameMeta.get(originalTableName).get(p);
			
			getMetaDbms().deleteSampleNameEntryFromDBMS(originalTableName.schemaName, originalTableName.tableName,
					sampleType, samplingRatio, columnNames,	getMetaNameTableName(originalTableName));
			getMetaDbms().deleteSampleSizeEntryFromDBMS(sampleTableName.schemaName, sampleTableName.tableName,
					getMetaSizeTableName(sampleTableName));
		} else {
			VerdictLogger.warn(String.format("No sample table for the parameter: [%s, %s, %.4f, %s]",
					originalTableName, sampleType, samplingRatio, columnNames.toString()));
		}
	}
	
	public void refreshSampleInfoIfNeeded(String schemaName) {
		if (!uptodateSchemas.contains(Pair.of(vc.getCurrentQid(), schemaName))) {
			refreshSampleInfo(schemaName);
			uptodateSchemas.add(Pair.of(vc.getCurrentQid(), schemaName));
		}
	}
	
	public void refreshSampleInfo(String schemaName) {
		ResultSet rs;
		
		TableUniqueName metaNameTable = getMetaNameTableName(schemaName);
		TableUniqueName metaSizeTable = getMetaSizeTableName(schemaName);
		
		try {
			// sample name
			String sql = String.format("SELECT originalschemaname, originaltablename, sampleschemaaname, sampletablename, "
					+ " sampletype, samplingratio, columnnames FROM %s", metaNameTable);
			rs = getMetaDbms().executeQuery(sql);
			while (rs.next()) {
				String originalSchemaName = rs.getString(1);
				String originalTabName = rs.getString(2);
				String sampleSchemaName = rs.getString(3);
				String sampleTabName = rs.getString(4);
				String sampleType = rs.getString(5);
				double samplingRatio = rs.getDouble(6);
				String columnNamesString = rs.getString(7);
				List<String> columnNames = (columnNamesString.length() == 0)?
						new ArrayList<String>() : Arrays.asList(columnNamesString.split(","));
				
				TableUniqueName originalTable = TableUniqueName.uname(originalSchemaName, originalTabName);
				if (!sampleNameMeta.containsKey(originalTable)) {
					sampleNameMeta.put(originalTable, new HashMap<SampleParam, TableUniqueName>());
				}
				sampleNameMeta.get(originalTable).put(
						new SampleParam(originalTable, sampleType, samplingRatio, columnNames),
						TableUniqueName.uname(sampleSchemaName, sampleTabName));
			}
			rs.close();
			
			// sample size
			sql = String.format("SELECT schemaname, tablename, samplesize, originaltablesize "
					+ " FROM %s", metaSizeTable);
			rs = getMetaDbms().executeQuery(sql);
			while (rs.next()) {
				String sampleSchemaName = rs.getString(1);
				String sampleTabName = rs.getString(2);
				Long sampleSize = rs.getLong(3);
				Long originalTableSize = rs.getLong(4);
				sampleSizeMeta.put(TableUniqueName.uname(sampleSchemaName, sampleTabName),
						new SampleSizeInfo(sampleSize, originalTableSize));
			}
			rs.close();
			
			// tables and their column names
			rs = getMetaDbms().getAllTableAndColumns(schemaName);
			while (rs.next()) {
				String tableName = rs.getString(1);
				String columnName = rs.getString(2);
				TableUniqueName tableUName = TableUniqueName.uname(schemaName, tableName); 
				if (!tableToColumnNames.containsKey(tableUName)) {
					tableToColumnNames.put(tableUName, new ArrayList<String>());
				}
				tableToColumnNames.get(tableUName).add(columnName);
			}
			rs.close();
			
		} catch (VerdictException | SQLException e) {
//			VerdictLogger.warn(e);
		}

		VerdictLogger.debug(this, "Sample meta data refreshed.");
	}
	
	public Pair<Long, Long> getSampleAndOriginalTableSizeBySampleTableNameIfExists(TableUniqueName sampleTableName) {
		refreshSampleInfoIfNeeded(sampleTableName.schemaName);
		if (sampleSizeMeta.containsKey(sampleTableName)) {
			SampleSizeInfo info = sampleSizeMeta.get(sampleTableName);
			return Pair.of(info.sampleSize, info.originalTableSize);
		} else {
			return Pair.of(-1L, -1L);
		}
	}
	
	/**
	 * Returns the sample creation parameters and the names of the created samples for a given original table.
	 * @param originalTableName
	 * @return A list of sample creation parameters and a sample table name.
	 */
	public List<Pair<SampleParam, TableUniqueName>> getSampleInfoFor(TableUniqueName originalTableName) {
		refreshSampleInfoIfNeeded(originalTableName.schemaName);
		List<Pair<SampleParam, TableUniqueName>> sampleInfo = new ArrayList<Pair<SampleParam, TableUniqueName>>();
		if (sampleNameMeta.containsKey(originalTableName)) {
			for (Map.Entry<SampleParam, TableUniqueName> e : sampleNameMeta.get(originalTableName).entrySet()) {
				sampleInfo.add(Pair.of(e.getKey(), e.getValue()));
			}
		}
		return sampleInfo;
	}
	
	/**
	 * Returns a pair of the original table size and the sample size for a given sample name.
	 * @param sampleTableName
	 * @return
	 */
	public Pair<Long, Long> getSampleSizeOf(TableUniqueName sampleTableName) {
		SampleSizeInfo a = sampleSizeMeta.get(sampleTableName);
		return Pair.of(a.originalTableSize, a.sampleSize);
	}

//	public Pair<Long, Long> getSampleAndOriginalTableSizeByOriginalTableNameIfExists(TableUniqueName originalTableName) {
//		refreshSampleInfoIfNeeded(originalTableName);
//		
//		TableUniqueName sampleTableName = sampleTableUniqueNameOf(originalTableName);
//		
//		if (sampleSizeMeta.containsKey(sampleTableName)) {
//			SampleInfo info = sampleSizeMeta.get(sampleTableName);
//			return Pair.of(info.sampleSize, info.originalTableSize);
//		} else {
//			return Pair.of(-1L, -1L);
//		}
//	}

//	public TableUniqueName getSampleTableNameIfExistsElseOriginal(TableUniqueName originalTableName) {
//		refreshSampleInfoIfNeeded(originalTableName.schemaName);
//		
//		if (sampleNameMeta.containsKey(originalTableName)) {
//			return sampleNameMeta.get(originalTableName);
//		} else {
//			return originalTableName;
//		}
//	}
	
	/**
	 * Obtains the name of the sample table for the given original table. This function performs a syntactic transformation,
	 * without semantic checks.
	 * @param originalTableName
	 * @return
	 */
	public TableUniqueName newSampleTableUniqueNameOf(TableUniqueName originalTableName) {
		String currentTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
		String localTableName = String.format("sample_%s_%s", originalTableName.tableName, currentTime);
		return TableUniqueName.uname(originalTableName.schemaName, localTableName);
	}
	
	public TableUniqueName newSampleTableUniqueNameOf(String originalTableName) {
		return newSampleTableUniqueNameOf(TableUniqueName.uname(vc, originalTableName));
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


class SampleSizeInfo {
	public long sampleSize = 0;
	public long originalTableSize = 0;
	
	public SampleSizeInfo(long sampleSize, long originalTableSize) {
		this.sampleSize = sampleSize;
		this.originalTableSize = originalTableSize;
	}
	
	@Override
	public String toString() {
		return String.format("sample (%d out of %d)", sampleSize, originalTableSize);
	}
}
