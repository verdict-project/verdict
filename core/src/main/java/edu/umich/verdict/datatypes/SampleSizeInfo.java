package edu.umich.verdict.datatypes;


public class SampleSizeInfo {
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
