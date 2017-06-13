package edu.umich.verdict.relation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.functions.AggFunction;
import edu.umich.verdict.util.VerdictLogger;
import static edu.umich.verdict.relation.functions.AggFunction.*;

/**
 * Aggregation functions issued on this table return approximate answers.
 * @author Yongjoo Park
 *
 */
public class ApproxRelation extends Relation {
	
	protected TableUniqueName sampleTableName;
	
	protected SampleParam param;
	
	protected SampleSizeInfo info;

	public ApproxRelation(VerdictContext vc, TableUniqueName originalTable, SampleParam param) {
		super(vc, originalTable);
		List<Pair<SampleParam, TableUniqueName>> sampleInfo = vc.getMeta().getSampleInfoFor(originalTable);
		for (Pair<SampleParam, TableUniqueName> e : sampleInfo) {
			SampleParam p = e.getLeft();
			
			if (p.samplingRatio == param.samplingRatio && p.sampleType.equals(param.sampleType) && p.columnNames.equals(param.columnNames)) {
				sampleTableName = e.getRight();
			}
		}
		
		if (sampleTableName != null) {
			info = vc.getMeta().getSampleSizeOf(sampleTableName);
		}
		
		if (sampleTableName == null) {
			if (param.sampleType.equals("universe") || param.sampleType.equals("stratified")) {
				VerdictLogger.error(this, String.format("No %.2f%% %s sample table on %s found for the table %s.",
						param.samplingRatio*100, param.sampleType, param.columnNames.toString(), originalTable));
			} else if (param.sampleType.equals("uniform")) {
				VerdictLogger.error(this, String.format("No %.2f%% %s sample table found for the table %s.",
						param.samplingRatio*100, param.sampleType, originalTable));
			}
		}
	}
	
	public TableUniqueName getOriginalTable() {
		return tableName;
	}
	
	public AggregateRelation aggOnSample(List<AggFunction> functions) {
		return new AggregateRelation(vc, this, functions);
	}
	
	public AggregateRelation aggOnSample(AggFunction... functions) {
		return aggOnSample(Arrays.asList(functions));
	}
	
	public long countOnSample() throws VerdictException {
		AggregateRelation r = aggOnSample(count());
		List<List<Object>> rs = r.collect();
		return (Long) rs.get(0).get(0);
	}
	
	public long countDistinctOnSample(String expression) throws VerdictException {
		AggregateRelation r = aggOnSample(countDistinct(expression));
		List<List<Object>> rs = r.collect();
		return (Long) rs.get(0).get(0);
	}
	
	/**
	 * Computes aggregate function answers that could be obtained when they were run on the original table.
	 * @param functions
	 * @return
	 */
	public ApproxAggregateRelation agg(AggFunction... functions) {
		return new ApproxAggregateRelation(vc, this, Arrays.asList(functions));
	}
	
	@Override
	public boolean isApproximate() {
		return true;
	}

}
