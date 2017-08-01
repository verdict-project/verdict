package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Represents a sample table before any projection (including aggregation) or filtering is performed.
 * Aggregation functions issued on this table return approximate answers.
 * @author Yongjoo Park
 *
 */
public class ApproxSingleRelation extends ApproxRelation {
	
	protected TableUniqueName sampleTableName;
	
	protected SampleParam param;
	
	protected SampleSizeInfo info;
	
	protected boolean derived;
	
	protected ApproxSingleRelation(VerdictContext vc, TableUniqueName sampleTableName, SampleParam param, SampleSizeInfo info) {
		super(vc);
		this.sampleTableName = sampleTableName;
		this.param = param;
		this.info = info;
		derived = false;
	}
	
	protected ApproxSingleRelation(VerdictContext vc, SampleParam param) {
		super(vc);
		this.param = param;
		if (param.getSampleType().equals("nosample")) {
			this.sampleTableName = param.getOriginalTable();
		} else {
			this.sampleTableName = vc.getMeta().lookForSampleTable(param);
			if (this.sampleTableName == null) {
				this.sampleTableName = TableUniqueName.uname(vc,
						param.toString().replace("(",  "").replace(")", "").replace(",", "") + "_does_not_exist");
			}
		}
		this.info = vc.getMeta().getSampleSizeOf(sampleTableName);
	}
	
	/**
	 * Uses this method when creating a sample table that points to the materialized table. Using this method sets
	 * the derived field to false.
	 * @param vc
	 * @param param
	 * @return
	 */
	public static ApproxSingleRelation from(VerdictContext vc, SampleParam param) {
		if (param.sampleType.equals("nosample")) {
			return asis(SingleRelation.from(vc, param.originalTable));
		} else {
			return new ApproxSingleRelation(vc, param);
		}
	}
	
	public static ApproxSingleRelation asis(SingleRelation r) {
		return new ApproxSingleRelation(
				r.vc,
				r.getTableName(), 
				new SampleParam(r.vc, r.getTableName(), "nosample", 1.0, null),
				new SampleSizeInfo(-1, -1));
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

	public TableUniqueName getOriginalTableName() {
		return getTableName();
	}
	
	public TableUniqueName getTableName() {
		return param.originalTable;
	}
	
	/*
	 * Approx
	 */

	@Override
	public ExactRelation rewriteForPointEstimate() {
		ExactRelation r = SingleRelation.from(vc, getSampleName());
		r.setAlias(getAlias());
		return r;
	}
	
	/**
	 * No Approximation is performed when this method is called directly.
	 */
	@Override
	public ExactRelation rewriteWithSubsampledErrorBounds() {
		return SingleRelation.from(vc, getOriginalTableName());
	}

	@Override
	public ExactRelation rewriteWithPartition() {
		if (param.sampleType.equals("universe")) {
			Set<String> colNames = vc.getMeta().getColumns(param.sampleTableName());
			String partitionColName = partitionColumnName();
			int partitionCount = 100;
			
			// we will create a new partition column using a hash function, so discard an existing one.
			List<String> newColNames = new ArrayList<String>();
			for (String c : colNames) {
				if (!c.equals(partitionColName)) {
					newColNames.add(c);
				}
			}
			
			// a new relation
			ExactRelation r = SingleRelation.from(vc, getSampleName());
			r = r.select(Joiner.on(", ").join(newColNames) + ", "
				     	 + vc.getDbms().modOfHash(param.columnNames.get(0), partitionCount) + " AS " + partitionColName);
			r.setAlias(getAlias());
			return r;
			
		} else {
			ExactRelation r = SingleRelation.from(vc, getSampleName());
			r.setAlias(getAlias());
			return r;
		}
//		r = vc.getDbms().augmentWithRandomPartitionNum(r);
//		r.setAliasName(getAliasName());
//		return r;
	}
	
//	@Override
//	public ColNameExpr partitionColumn() {
//		String col = partitionColumnName();
//		return new ColNameExpr(col, getTableName().tableName);
//	}

	@Override
	protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
		if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
			if (getSampleType().equals("universe")) {
				return Arrays.<Expr>asList(ConstantExpr.from(getSamplingRatio()));
			} else if (getSampleType().equals("stratified")) {
				return Arrays.<Expr>asList(ConstantExpr.from(1.0));
			} else if (getSampleType().equals("nosample")) {
				return Arrays.<Expr>asList(ConstantExpr.from(1.0));
			} else {
				VerdictLogger.warn(this, String.format("%s sample should not be used for count-distinct.", getSampleType()));
				return Arrays.<Expr>asList(ConstantExpr.from(1.0));
			}
		} else {	// SUM, COUNT
			if (!getSampleType().equals("nosample")) {
				String samplingProbCol = samplingProbabilityColumnName();
				return Arrays.<Expr>asList(new ColNameExpr(samplingProbCol, alias));
			} else {
				return Arrays.<Expr>asList();
			}
		}
	}
	
	@Override
	public String sampleType() {
		return getSampleType();
	}
	
	@Override
	public double cost() {
		if (sampleType().equals("nosample")) {
			SampleParam ufParam = new SampleParam(vc, param.getOriginalTable(), "uniform", null, Arrays.<String>asList());
			TableUniqueName ufSample = vc.getMeta().lookForSampleTable(ufParam);
			SampleSizeInfo info = vc.getMeta().getSampleSizeOf(ufSample);
			return (info == null)? 1e9 : info.originalTableSize;
		} else {
			SampleSizeInfo info = vc.getMeta().getSampleSizeOf(param);
			if (info == null) {
				return -1;
			}
			return info.sampleSize;
		}
	}
	
	@Override
	protected List<String> sampleColumns() {
		return param.columnNames;
	}

	/**
	 * Using this substitution pattern can handle:
	 * 1. user specified his own table alias and using it: no need for substitution since aliases are preserved.
	 * 2. user specified his own table alias but referring the raw table name: below pattern handles it.
	 * 3. user didn't specified table aliases: below pattern handles it.
	 */
	@Override
	protected Map<TableUniqueName, String> tableSubstitution() {
		Map<TableUniqueName, String> s = ImmutableMap.of(param.originalTable, alias);
		return s;
	}
	
	/*
	 * Aggregations
	 */
	
	public ExactRelation aggOnSample(List<Object> functions) {
		return rewriteForPointEstimate().agg(functions);
	}
	
	public ExactRelation aggOnSample(Object... functions) {
		return aggOnSample(Arrays.asList(functions));
	}
	
	public long countOnSample() throws VerdictException {
		ExactRelation r = aggOnSample(FuncExpr.count());
		List<List<Object>> rs = r.collect();
		return (Long) rs.get(0).get(0);
	}
	
	public long countDistinctOnSample(Expr expression) throws VerdictException {
		ExactRelation r = aggOnSample(FuncExpr.countDistinct(expression));
		List<List<Object>> rs = r.collect();
		return (Long) rs.get(0).get(0);
	}
	
	@Override
	public String toString() {
		return "ApproxSingleRelation(" + param.toString() + ")";
	}

	@Override
	public int hashCode() {
		return sampleTableName.hashCode();
	}

	@Override
	public boolean equals(Object a) {
		if (a instanceof ApproxSingleRelation) {
			return sampleTableName.equals(((ApproxSingleRelation) a).getSampleName());
		} else {
			return false;
		}
	}
	
	@Override
	protected String toStringWithIndent(String indent) {
		StringBuilder s = new StringBuilder(1000);
		s.append(indent);
		s.append(String.format("%s(%s, %s), %s, cost: %f\n",
				this.getClass().getSimpleName(),
				getTableName(),
				getAlias(),
				param.toString(),
				cost()));
		return s.toString();
	}
	
	@Override
	public boolean equals(ApproxRelation o) {
		if (o instanceof ApproxSingleRelation) {
			return param.equals(((ApproxSingleRelation) o).param);
		} else {
			return false;
		}
	}

	@Override
	public double samplingProbability() {
		return param.getSamplingRatio();
	}

    @Override
    protected boolean doesIncludeSample() {
        if (!sampleType().equals("nosample")) {
            return true;
        } else {
            return false;
        }
    }
}
