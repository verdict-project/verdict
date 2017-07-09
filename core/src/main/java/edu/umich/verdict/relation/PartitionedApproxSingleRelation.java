package edu.umich.verdict.relation;

import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;

public class PartitionedApproxSingleRelation extends ApproxSingleRelation {

	protected PartitionedApproxSingleRelation(VerdictJDBCContext vc, SampleParam param) {
		super(vc, param);
	}

	protected PartitionedApproxSingleRelation(VerdictJDBCContext vc, TableUniqueName sampleTableName, SampleParam param,
			SampleSizeInfo info) {
		super(vc, sampleTableName, param, info);
	}

}
