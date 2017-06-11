package edu.umich.verdict.datatypes;

import java.util.List;


public class SampleParam {

	public TableUniqueName originalTable;
	public String sampleType;
	public Double samplingRatio;
	public List<String> columnNames;
	
	public SampleParam(TableUniqueName originalTable, String sampleType, double samplingRatio, List<String> columnNames) {
		this.originalTable = originalTable;
		this.sampleType = sampleType;
		this.samplingRatio = samplingRatio;
		this.columnNames = columnNames;
	}
	
	@Override
	public int hashCode() {
		return originalTable.hashCode() + sampleType.hashCode() + samplingRatio.hashCode() + columnNames.hashCode();
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
