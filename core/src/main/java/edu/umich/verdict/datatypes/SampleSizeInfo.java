package edu.umich.verdict.datatypes;

import java.util.List;

public class SampleSizeInfo {
	public long sampleSize = 0;
	public long originalTableSize = 0;
	public String sampleType;
	public Double samplingRatio;
	public List<String> columnNames;
	
	public SampleSizeInfo(long sampleSize, long originalTableSize, String sampleType, double samplingRatio, List<String> columnNames) {
		this.sampleSize = sampleSize;
		this.originalTableSize = originalTableSize;
		this.sampleType = sampleType;
		this.samplingRatio = samplingRatio;
		this.columnNames = columnNames;
	}
	
	@Override
	public String toString() {
		return String.format("sample (%d out of %d)", sampleSize, originalTableSize);
	}
}
