package edu.umich.verdict.relation;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.TypeCasting;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Represents a sample table before any projection (including aggregation) or filtering is performed.
 * Aggregation functions issued on this table return approximate answers.
 * @author Yongjoo Park
 *
 */
public class SampleRelation extends ExactRelation implements Relation, ApproxRelation {
	
	protected TableUniqueName sampleTableName;
	
	protected SampleParam param;
	
	protected SampleSizeInfo info;
	
	protected boolean derived;

	protected SampleRelation(VerdictContext vc, SampleParam param) {
		super(vc, param.originalTable);
		this.param = param;
		this.derived = true;
		
		// set this.info
		TableUniqueName originalTable = param.originalTable;
		List<Pair<SampleParam, TableUniqueName>> sampleInfo = vc.getMeta().getSampleInfoFor(originalTable);
		for (Pair<SampleParam, TableUniqueName> e : sampleInfo) {
			SampleParam p = e.getLeft();
			
			if (param.samplingRatio == null) {
				if (p.originalTable.equals(param.originalTable)
						&& p.sampleType.equals(param.sampleType)
						&& p.columnNames.equals(param.columnNames)) {
					sampleTableName = e.getRight();
				}
			} else {
				if (p.equals(param)) {
					sampleTableName = e.getRight();
				}
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
	
	/**
	 * Uses this method when creating a sample table that points to the materialized table. Using this method sets
	 * the derived field to false.
	 * @param vc
	 * @param param
	 * @return
	 */
	public static SampleRelation from(VerdictContext vc, SampleParam param) {
		SampleRelation r = new SampleRelation(vc, param);
		r.setDerivedTable(false);
		return r;
	}
	
	public TableUniqueName getOriginalTableName() {
		return getTableName();
	}
	
	public boolean isDerivedTable() {
		return derived;
	}
	
	public void setDerivedTable(boolean a) {
		derived = a;
	}
	
	public ExactRelation sampleAsExactRelation() {
		return ExactRelation.from(vc, getSampleName());
	}
	
	public AggregatedRelation aggOnSample(List<Expr> functions) {
		return sampleAsExactRelation().agg(functions);
	}
	
	public AggregatedRelation aggOnSample(Expr... functions) {
		return aggOnSample(Arrays.asList(functions));
	}
	
	public long countOnSample() throws VerdictException {
		Relation r = aggOnSample(FuncExpr.count());
		List<List<Object>> rs = r.collect();
		return (Long) rs.get(0).get(0);
	}
	
	public long countDistinctOnSample(Expr expression) throws VerdictException {
		Relation r = aggOnSample(FuncExpr.countDistinct(expression));
		List<List<Object>> rs = r.collect();
		return (Long) rs.get(0).get(0);
	}
	
	/**
	 * Computes aggregate function answers that could be obtained when they were run on the original table.
	 * @param functions
	 * @return
	 */
	public AggregatedSampleRelation agg(Expr... functions) {
		return agg(Arrays.asList(functions));
	}
	
	public AggregatedSampleRelation agg(List<Expr> functions) {
		return new AggregatedSampleRelation(vc, this, functions);
	}
	
	public long count() throws VerdictException {
		return TypeCasting.toLongint(agg(FuncExpr.count()).transform().collect().get(0).get(0));
	}
	
	@Override
	public boolean isApproximate() {
		return true;
	}

	public TableUniqueName getSampleName() {
		return sampleTableName;
	}
	
	public long getSampleSize() {
		return info.sampleSize;
	}
	
	public long getOriginalTableSize() {
		return info.originalTableSize;
	}
	
	public double getSamplingRatio() {
		return param.samplingRatio;
	}
	
	public String getSampleType() {
		return param.sampleType;
	}
	
	@Override
	public String toSql() throws VerdictException {
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT * FROM " + sampleTableName);
		return sql.toString();
	}

}
