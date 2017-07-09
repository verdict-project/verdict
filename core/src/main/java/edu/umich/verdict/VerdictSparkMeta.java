package edu.umich.verdict;

import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class VerdictSparkMeta extends VerdictMeta {
	
	protected VerdictSparkContext vc;

	public VerdictSparkMeta(VerdictSparkContext vc) throws VerdictException {
		super();
		this.vc = vc;
	}

	@Override
	public void insertSampleInfo(SampleParam param, long sampleSize, long originalTableSize) throws VerdictException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void refreshSampleInfo(String schemaName) {
		TableUniqueName metaNameTable = getMetaNameTableForOriginalSchema(schemaName);
		TableUniqueName metaSizeTable = getMetaSizeTableForOriginalSchema(schemaName);
		
		// tables and column names
		
		
	}

}
