package edu.umich.verdict;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.util.VerdictLogger;

public class VerdictJDBCMeta extends VerdictMeta {
	
	protected VerdictJDBCContext vc;
	
	public VerdictJDBCMeta(VerdictJDBCContext vc) throws VerdictException {
		super();
		this.vc = vc;
	}
	
	@Override
	public void insertSampleInfo(SampleParam param, long sampleSize, long originalTableSize) throws VerdictException {
		TableUniqueName fullSampleName = param.sampleTableName();
		
		vc.getMetaDbms().createMetaTablesInDMBS(param.originalTable,
				getMetaSizeTableForSampleTable(fullSampleName),
				getMetaNameTableForSampleTable(fullSampleName));
		
		getMetaDbms().updateSampleNameEntryIntoDBMS(param, getMetaNameTableForSampleTable(fullSampleName));
		
		getMetaDbms().updateSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, getMetaSizeTableForSampleTable(fullSampleName));
	}
	
	public void refreshSampleInfo(String schemaName) {
		ResultSet rs;
		
		TableUniqueName metaNameTable = getMetaNameTableForOriginalSchema(schemaName);
		TableUniqueName metaSizeTable = getMetaSizeTableForOriginalSchema(schemaName);
		
		try {
			// tables and their column names (we get both of the current schema and its meta schema)
			// current schema
			tableToColumnNames.clear();
			populateTableAndColumnInfoFor(schemaName);
			
			// meta schema
			String metaSchema = metaCatalogForDataCatalog(schemaName);
			if (!metaSchema.equals(schemaName)) {
				populateTableAndColumnInfoFor(metaSchema);
			}
			
			sampleNameMeta.clear();
			if (tableToColumnNames.containsKey(metaNameTable)) {
				// sample name
				rs = SingleRelation.from(vc, metaNameTable)
					 .select("originalschemaname, originaltablename, sampleschemaaname, sampletablename, sampletype, samplingratio, columnnames")
				     .collectResultSet();
				while (rs.next()) {
					String originalSchemaName = rs.getString(1);
					String originalTabName = rs.getString(2);
					String sampleSchemaName = rs.getString(3);
					String sampleTabName = rs.getString(4);
					String sampleType = rs.getString(5);
					double samplingRatio = rs.getDouble(6);
					String columnNamesString = rs.getString(7);
					List<String> columnNames = (columnNamesString.length() == 0)? new ArrayList<String>() : Arrays.asList(columnNamesString.split(","));

					TableUniqueName originalTable = TableUniqueName.uname(originalSchemaName, originalTabName);
					if (!sampleNameMeta.containsKey(originalTable)) {
						sampleNameMeta.put(originalTable, new HashMap<SampleParam, TableUniqueName>());
					}
					sampleNameMeta.get(originalTable).put(
							new SampleParam(vc, originalTable, sampleType, samplingRatio, columnNames),
							TableUniqueName.uname(sampleSchemaName, sampleTabName));
				}
				rs.close();
			}

			sampleSizeMeta.clear();
			if (tableToColumnNames.containsKey(metaSizeTable)) {
				// sample size
				rs = SingleRelation.from(vc, metaSizeTable)
					 .select("schemaname, tablename, samplesize, originaltablesize")
					 .collectResultSet();
				while (rs.next()) {
					String sampleSchemaName = rs.getString(1);
					String sampleTabName = rs.getString(2);
					Long sampleSize = rs.getLong(3);
					Long originalTableSize = rs.getLong(4);
					sampleSizeMeta.put(TableUniqueName.uname(sampleSchemaName, sampleTabName),
							new SampleSizeInfo(sampleSize, originalTableSize));
				}
				rs.close();
			}
		} catch (VerdictException | SQLException e) {
//			VerdictLogger.warn(e);
		}

		VerdictLogger.debug(this, "Sample meta data refreshed.");
	}
	
	private void populateTableAndColumnInfoFor(String schema) throws VerdictException {
		List<Pair<String, String>> tabCols = vc.getDbms().getAllTableAndColumns(schema);
		for (Pair<String, String> tabCol : tabCols) {
			TableUniqueName tableUName = TableUniqueName.uname(schema, tabCol.getLeft());
			if (!tableToColumnNames.containsKey(tableUName)) {
				tableToColumnNames.put(tableUName, new ArrayList<String>());
			}
			tableToColumnNames.get(tableUName).add(tabCol.getRight());
		}
	}
}
