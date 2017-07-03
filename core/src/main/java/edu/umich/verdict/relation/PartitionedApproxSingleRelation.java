package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;

public class PartitionedApproxSingleRelation extends ApproxSingleRelation {

	protected PartitionedApproxSingleRelation(VerdictContext vc, SampleParam param) {
		super(vc, param);
	}

	protected PartitionedApproxSingleRelation(VerdictContext vc, TableUniqueName sampleTableName, SampleParam param,
			SampleSizeInfo info) {
		super(vc, sampleTableName, param, info);
	}

}
