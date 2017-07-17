package edu.umich.verdict;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;


public class VerdictMeta {
	
	private final String META_SIZE_TABLE;
	
	private final String META_NAME_TABLE;
	
	/**
	 * Works as a cache for a single query execution.
	 * key: sample table
	 * value: sample size info
	 */
	protected Map<TableUniqueName, SampleSizeInfo> sampleSizeMeta;
	
	/**
	 * Works as a cache for a single query execution.
	 * key: original table
	 * value: key: sample creation params
	 * 	      value: sample table
	 */
	protected Map<TableUniqueName, Map<SampleParam, TableUniqueName>> sampleNameMeta;
	
	/**
	 * remembers for what query id and schema, we have updated the meta info.
	 */
	protected Map<String, Long> uptodateSchemas;
	
	/**
	 * remembers tables and their column names.
	 */
//	protected Map<TableUniqueName, List<String>> tableToColumnNames;
	
	protected Set<String> databases;		// a.k.a. database names.
	
	protected Map<String, Set<String>> tables;
	
	protected Map<TableUniqueName, Set<String>> columns;
	
	protected VerdictContext vc;

	public VerdictMeta(VerdictContext vc) {
		this.vc = vc;
		sampleSizeMeta = new HashMap<TableUniqueName, SampleSizeInfo>();
		sampleNameMeta = new HashMap<TableUniqueName, Map<SampleParam, TableUniqueName>>();
		uptodateSchemas = new HashMap<String, Long>();
		databases = new HashSet<String>();
		tables = new HashMap<String, Set<String>>();
		columns = new HashMap<TableUniqueName, Set<String>>();
//		tableToColumnNames = new HashMap<TableUniqueName, List<String>>();
		META_NAME_TABLE = vc.getConf().get("verdict.meta_name_table");
		META_SIZE_TABLE = vc.getConf().get("verdict.meta_size_table");
	}
	
	protected Dbms getMetaDbms() {
		return vc.getMetaDbms();
	}
	
	public void clearSampleInfo() {
		uptodateSchemas.clear();
		sampleSizeMeta.clear();
		sampleNameMeta.clear();
		databases.clear();
		tables.clear();
		columns.clear();
	}
	
	/**
	 * retrieves cached database names.
	 * @return
	 */
	public Set<String> getDatabases() {
		if (databases.isEmpty()) {
			refreshDatabases();
		}
		return databases;
	}
	
	public void refreshDatabases() {
		try {
			Set<String> databases = vc.getDbms().getDatabases();
			this.databases = databases;
		} catch (VerdictException e) {
			VerdictLogger.error(e);
		}
	}
	
	public Set<String> getTables(String database) {
		if (!tables.containsKey(database)) {
			refreshTables(database);
		}
		return tables.get(database);
	}
	
	public void refreshTables(String database) {
		try {
			List<String> tables = vc.getDbms().getTables(database);
			this.tables.put(database, new TreeSet<String>(tables));
		} catch (VerdictException e) {
			VerdictLogger.error(e);
		}
	}
	
	public Set<String> getColumns(TableUniqueName tableName) {
		if (!columns.containsKey(tableName)) {
			refreshColumns(tableName);
		}
		return columns.get(tableName);
	}
	
	public void refreshColumns(TableUniqueName tableName) {
		try {
			List<String> columns = vc.getDbms().getColumns(tableName);
			this.columns.put(tableName, new TreeSet<String>(columns));
		} catch (VerdictException e) {
			VerdictLogger.error(e);
		}
	}
	
//	public List<String> getColumnNames(TableUniqueName tableName) {
//		refreshSampleInfoIfNeeded(tableName.getSchemaName());
//		if (tableToColumnNames.containsKey(tableName)) {
//			return tableToColumnNames.get(tableName);
//		} else {
//			return new ArrayList<String>();
//		}
//	}
	
//	public Map<TableUniqueName, List<String>> getTableAndColumnNames(String schemaName) {
//		refreshSampleInfoIfNeeded(schemaName);
//		Map<TableUniqueName, List<String>> inSchema = new HashMap<TableUniqueName, List<String>>();
//		for (Map.Entry<TableUniqueName, List<String>> entry : tableToColumnNames.entrySet()) {
//			TableUniqueName table = entry.getKey();
//			List<String> columns = entry.getValue();
//			if (table.getSchemaName().equals(schemaName)) {
//				inSchema.put(table, columns);
//			}
//		}
//		return inSchema;
//	}
	
	/**
	 * Insert sample info into local data structure (for quick access) and into the DBMS (for persistence).
	 * @param originalSchemaName
	 * @param originalTableName
	 * @param sampleSize
	 * @param originalTableSize
	 * @throws VerdictException 
	 */
	public void insertSampleInfo(SampleParam param, long sampleSize, long originalTableSize) throws VerdictException {
		TableUniqueName fullSampleName = param.sampleTableName();
		
		vc.getMetaDbms().createMetaTablesInDMBS(param.originalTable,
				getMetaSizeTableForSampleTable(fullSampleName),
				getMetaNameTableForSampleTable(fullSampleName));
		
		getMetaDbms().updateSampleNameEntryIntoDBMS(param, getMetaNameTableForSampleTable(fullSampleName));
		
		getMetaDbms().updateSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, getMetaSizeTableForSampleTable(fullSampleName));
	}
		
	/**
	 * Delete sample info from {@link #META_SIZE_TABLE} (for quick access) and from the DBMS (for persistence).
	 * @param originalTableName
	 * @throws VerdictException 
	 */
	public void deleteSampleInfo(SampleParam param) throws VerdictException {
		refreshSampleInfoIfNeeded(param.originalTable.getSchemaName());
		TableUniqueName originalTable = param.originalTable;
		
		if (sampleNameMeta.containsKey(originalTable)) {
			TableUniqueName sampleTableName = sampleNameMeta.get(originalTable).get(param);
			getMetaDbms().deleteSampleNameEntryFromDBMS(param, getMetaNameTableForOriginalTable(originalTable));
			getMetaDbms().deleteSampleSizeEntryFromDBMS(param, getMetaSizeTableForSampleTable(sampleTableName));
		} else {
			VerdictLogger.warn(String.format("No sample table for the parameter: [%s, %s, %.4f, %s]",
					param.originalTable, param.sampleType, param.samplingRatio, param.columnNames.toString()));
		}
	}
	
	public void refreshSampleInfoIfNeeded(String schemaName) {
		boolean needToRefresh = false;
		String refreshOption = vc.getConf().get("verdict.refresh_meta");
		
		if (refreshOption.equals("per_session")) {
			if (!uptodateSchemas.containsKey(schemaName)) {
				needToRefresh = true;
			}
		} else if (refreshOption.equals("per_query")) {
			// update if the last time when schemaName was updated is before the current qid.
			if (!uptodateSchemas.containsKey(schemaName)) {
				needToRefresh = true;
			} else {
				if (uptodateSchemas.get(schemaName) < vc.getCurrentQid()) {
					needToRefresh = true;
				}
			}
		} else if (refreshOption.equals("manual")) {
			// don't do anything
		}
		
		if (needToRefresh) {
			refreshSampleInfo(schemaName);
		}
	}
	
	public void refreshSampleInfo(String schemaName) {
		TableUniqueName metaNameTable = getMetaNameTableForOriginalSchema(schemaName);
		TableUniqueName metaSizeTable = getMetaSizeTableForOriginalSchema(schemaName);
		List<List<Object>> result;
		
		try {
			// tables and their column names (we get both of the current schema and its meta schema)
			// current schema
//			tableToColumnNames.clear();
//			populateTableAndColumnInfoFor(schemaName);
//			
//			// meta schema
//			String metaSchema = metaCatalogForDataCatalog(schemaName);
//			if (!metaSchema.equals(schemaName)) {
//				populateTableAndColumnInfoFor(metaSchema);
//				Set<String> databases = vc.getDbms().getDatabases();
//				if (databases.contains(metaSchema)) {
//					uptodateSchemas.put(metaSchema, vc.getCurrentQid());
//				}
//			}
			
			sampleNameMeta.clear();
			sampleSizeMeta.clear();
			
			Set<String> databases = getDatabases();
			if (databases.contains(metaNameTable.getSchemaName())) {
				Set<String> tables = getTables(metaNameTable.getSchemaName());
				if (tables != null && tables.contains(metaNameTable.getTableName())) {
					vc.getDbms().cacheTable(metaNameTable);
					
					// sample name
					result = SingleRelation.from(vc, metaNameTable)
							.select("originalschemaname, originaltablename, sampleschemaaname, sampletablename, sampletype, samplingratio, columnnames")
							.collect();
					for (List<Object> row : result) {
						String originalSchemaName = row.get(0).toString();
						String originalTabName = row.get(1).toString();
						String sampleSchemaName = row.get(2).toString();
						String sampleTabName = row.get(3).toString();
						String sampleType = row.get(4).toString();
						double samplingRatio = TypeCasting.toDouble(row.get(5));
						String columnNamesString = row.get(6).toString();
						List<String> columnNames = (columnNamesString.length() == 0)? new ArrayList<String>() : Arrays.asList(columnNamesString.split(","));
						
						TableUniqueName originalTable = TableUniqueName.uname(originalSchemaName, originalTabName);
						if (!sampleNameMeta.containsKey(originalTable)) {
							sampleNameMeta.put(originalTable, new HashMap<SampleParam, TableUniqueName>());
						}
						sampleNameMeta.get(originalTable).put(
								new SampleParam(vc, originalTable, sampleType, samplingRatio, columnNames),
								TableUniqueName.uname(sampleSchemaName, sampleTabName));
						
						vc.getDbms().cacheTable(TableUniqueName.uname(sampleSchemaName, sampleTabName));
					}
				}
			}
			
			if (databases.contains(metaSizeTable.getSchemaName())) {
				Set<String> tables = getTables(metaSizeTable.getSchemaName());
				if (tables != null && tables.contains(metaSizeTable.getTableName())) {
					vc.getDbms().cacheTable(metaSizeTable);
					
					// sample size
					result = SingleRelation.from(vc, metaSizeTable)
							.select("schemaname, tablename, samplesize, originaltablesize")
							.collect();
					for (List<Object> row : result) {
						String sampleSchemaName = row.get(0).toString();
						String sampleTabName = row.get(1).toString();
						Long sampleSize = TypeCasting.toLong(row.get(2));
						Long originalTableSize = TypeCasting.toLong(row.get(3));
						sampleSizeMeta.put(TableUniqueName.uname(sampleSchemaName, sampleTabName),
								new SampleSizeInfo(sampleSize, originalTableSize));
						
						vc.getDbms().cacheTable(TableUniqueName.uname(sampleSchemaName, sampleTabName));
					}
				}
			}
		} catch (VerdictException e) {
			VerdictLogger.error(this, e.getMessage());
		}
		
		uptodateSchemas.put(schemaName, vc.getCurrentQid());
		VerdictLogger.info(this, "Verdict meta data was refreshed.");
	}
	
//	private void populateTableAndColumnInfoFor(String schema) throws VerdictException {
//		List<Pair<String, String>> tabCols = vc.getDbms().getAllTableAndColumns(schema);
//		for (Pair<String, String> tabCol : tabCols) {
//			TableUniqueName tableUName = TableUniqueName.uname(schema, tabCol.getLeft());
//			if (!tableToColumnNames.containsKey(tableUName)) {
//				tableToColumnNames.put(tableUName, new ArrayList<String>());
//			}
//			tableToColumnNames.get(tableUName).add(tabCol.getRight());
//		}
//	}
	
//	public Pair<Long, Long> getSampleAndOriginalTableSizeBySampleTableNameIfExists(TableUniqueName sampleTableName) {
//		refreshSampleInfoIfNeeded(sampleTableName.getSchemaName());
//		if (sampleSizeMeta.containsKey(sampleTableName)) {
//			SampleSizeInfo info = sampleSizeMeta.get(sampleTableName);
//			return Pair.of(info.sampleSize, info.originalTableSize);
//		} else {
//			return Pair.of(-1L, -1L);
//		}
//	}
	
	/**
	 * Returns the sample creation parameters and the names of the created samples for a given original table.
	 * @param originalTableName
	 * @return A list of sample creation parameters and a sample table name.
	 */
	public List<Pair<SampleParam, TableUniqueName>> getSampleInfoFor(TableUniqueName originalTableName) {
		refreshSampleInfoIfNeeded(originalTableName.getSchemaName());
		List<Pair<SampleParam, TableUniqueName>> sampleInfo = new ArrayList<Pair<SampleParam, TableUniqueName>>();
		if (sampleNameMeta.containsKey(originalTableName)) {
			for (Map.Entry<SampleParam, TableUniqueName> e : sampleNameMeta.get(originalTableName).entrySet()) {
				sampleInfo.add(Pair.of(e.getKey(), e.getValue()));
			}
		}
		return sampleInfo;
	}
	
	/**
	 * Returns the sample and original table size for the given sample table name.
	 * @param sampleTableName
	 * @return
	 */
	public SampleSizeInfo getSampleSizeOf(TableUniqueName sampleTableName) {
		return sampleSizeMeta.get(sampleTableName);
	}
	
	public SampleSizeInfo getSampleSizeOf(SampleParam param) {
		TableUniqueName sampleTable = lookForSampleTable(param);
		if (sampleTable == null) {
			return null;
		}
		return vc.getMeta().getSampleSizeOf(sampleTable);
	}
	
	public TableUniqueName lookForSampleTable(SampleParam param) {
		TableUniqueName originalTable = param.originalTable;
		List<Pair<SampleParam, TableUniqueName>> sampleInfo = vc.getMeta().getSampleInfoFor(originalTable);
		TableUniqueName sampleTable = null;
		
		for (Pair<SampleParam, TableUniqueName> e : sampleInfo) {
			SampleParam p = e.getLeft();
			
			if (param.samplingRatio == null) {
				if (p.originalTable.equals(param.originalTable)
						&& p.sampleType.equals(param.sampleType)
						&& p.columnNames.equals(param.columnNames)) {
					sampleTable = e.getRight();
				}
			} else {
				if (p.equals(param)) {
					sampleTable = e.getRight();
				}
			}
		}
		
//		if (sampleTable == null) {
//			if (param.sampleType.equals("universe") || param.sampleType.equals("stratified")) {
//				VerdictLogger.error(this, String.format("No %.2f%% %s sample table on %s found for the table %s.",
//						param.samplingRatio*100, param.sampleType, param.columnNames.toString(), originalTable));
//			} else if (param.sampleType.equals("uniform")) {
//				if (param.samplingRatio != null) {
//					VerdictLogger.error(this, String.format("No %.2f%% %s sample table found for the table %s.",
//							param.samplingRatio*100, param.sampleType, originalTable));
//				} else {
//					VerdictLogger.error(this, String.format("No %s sample table found for the table %s.",
//							param.sampleType, originalTable));
//				}
//			}
//		}
		
		return sampleTable;
	}
	
	/**
	 * 
	 * @param relatedTableName Either the original table or the sample table.
	 * @return
	 */
	public TableUniqueName getMetaSizeTableForOriginalTable(TableUniqueName originalTable) {
		return getMetaSizeTableForOriginalSchema(originalTable.getSchemaName());
	}
	
	public TableUniqueName getMetaSizeTableForOriginalSchema(String schema) {
		return TableUniqueName.uname(metaCatalogForDataCatalog(schema), META_SIZE_TABLE);
	}
	
	public TableUniqueName getMetaSizeTableForSampleTable(TableUniqueName sampleTable) {
		return getMetaSizeTableForSampleSchema(sampleTable.getSchemaName());
	}
	
	public TableUniqueName getMetaSizeTableForSampleSchema(String schema) {
		return TableUniqueName.uname(schema, META_SIZE_TABLE);
	}
	
	/**
	 * 
	 * @param relatedTableName Either the original table or the sample table.
	 * @return
	 */
	public TableUniqueName getMetaNameTableForOriginalTable(TableUniqueName originalTable) {
		return TableUniqueName.uname(metaCatalogForDataCatalog(originalTable.getSchemaName()), META_NAME_TABLE);
	}
	
	public TableUniqueName getMetaNameTableForOriginalSchema(String schema) {
		return TableUniqueName.uname(metaCatalogForDataCatalog(schema), META_NAME_TABLE);
	}
	
	public TableUniqueName getMetaNameTableForSampleTable(TableUniqueName sampleTable) {
		return TableUniqueName.uname(sampleTable.getSchemaName(), META_NAME_TABLE);
	}
	
	public TableUniqueName getMetaNameTableForSampleSchema(String schema) {
		return TableUniqueName.uname(schema, META_NAME_TABLE);
	}
	
	public String metaCatalogForDataCatalog(String dataCatalog) {
		return dataCatalog + vc.getConf().get("verdict.meta_catalog_suffix");
	}
	
}
