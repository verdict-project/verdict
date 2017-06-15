package edu.umich.verdict.datatypes;

import java.util.ArrayList;
import java.util.List;


public class SampleParam {

	public TableUniqueName originalTable;
	public String sampleType;
	public Double samplingRatio;
	public List<String> columnNames;
	
	public SampleParam(TableUniqueName originalTable, String sampleType, Double samplingRatio, List<String> columnNames) {
		this.originalTable = originalTable;
		this.sampleType = sampleType;
		this.samplingRatio = samplingRatio;
		if (columnNames == null) {
			this.columnNames = new ArrayList<String>();
		} else {
			this.columnNames = columnNames;
		}
	}
	
	@Override
	public int hashCode() {
		return originalTable.hashCode() + sampleType.hashCode() + samplingRatio.hashCode() + columnNames.hashCode();
	}
	
	public TableUniqueName sampleTableName() {
		String typeShortName = null;
		if (sampleType.equals("uniform")) {
			typeShortName = "uf";
		} else if (sampleType.equals("universe")) {
			typeShortName = "uv";
		} else if (sampleType.equals("stratified")) {
			typeShortName = "st";
		}
		
		StringBuilder colNames = new StringBuilder();
		if (columnNames.size() > 0) colNames.append("_");
		for (String n : columnNames) colNames.append(n);
		
		return TableUniqueName.uname(originalTable.schemaName,
				String.format("vs_%s_%s_%s", originalTable.tableName, typeShortName,
						String.format("%.4f", samplingRatio).replace('.', '_'))
				+ ((colNames.length() > 0)? colNames.toString() : ""));
	}
	
	@Override
	public boolean equals(Object another) {
		if (another instanceof SampleParam) {
			SampleParam t = (SampleParam) another;
			return originalTable.equals(t.originalTable) && sampleType.equals(t.sampleType)
					&& samplingRatio.equals(t.samplingRatio) && columnNames.equals(t.columnNames);
		} else {
			return false;
		}
	}
}
