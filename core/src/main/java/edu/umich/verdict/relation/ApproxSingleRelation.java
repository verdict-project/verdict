/*
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umich.verdict.relation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.umich.verdict.relation.expr.TableNameExpr;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Represents a sample table before any projection (including aggregation) or
 * filtering is performed. Aggregation functions issued on this table return
 * approximate answers.
 * 
 * @author Yongjoo Park
 *
 */
public class ApproxSingleRelation extends ApproxRelation {

    protected TableUniqueName sampleTableName;

    protected SampleParam param;

    protected SampleSizeInfo info;

    protected boolean derived;

    protected ApproxSingleRelation(VerdictContext vc, TableUniqueName sampleTableName, SampleParam param,
            SampleSizeInfo info) {
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
                        param.toString().replace("(", "").replace(")", "").replace(",", "") + "_does_not_exist");
            }
        }
        this.info = vc.getMeta().getSampleSizeOf(sampleTableName);
    }

    /**
     * Uses this method when creating a sample table that points to the materialized
     * table. Using this method sets the derived field to false.
     * 
     * @param vc
     * @param param
     * @return
     */
    public static ApproxSingleRelation from(VerdictContext vc, SampleParam param) {
        if (param.getSampleType().equals("nosample")) {
            return asis(SingleRelation.from(vc, param.getOriginalTable()));
        } else {
            return new ApproxSingleRelation(vc, param);
        }
    }

    public static ApproxSingleRelation asis(SingleRelation r) {
        ApproxSingleRelation a = new ApproxSingleRelation(r.vc, r.getTableName(),
                new SampleParam(r.vc, r.getTableName(), "nosample", 1.0, null), new SampleSizeInfo(r.getTableName(), -1, -1));
        a.setAlias(r.getAlias());
        return a;
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
        return param.getSamplingRatio();
    }

    public String getSampleType() {
        return param.getSampleType();
    }

    public TableUniqueName getOriginalTableName() {
        return getTableName();
    }

    public TableUniqueName getTableName() {
        return param.getOriginalTable();
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
        ExactRelation r = SingleRelation.from(vc, getOriginalTableName());
        r.setAlias(getAlias());
        return r;
    }

    @Override
    public ExactRelation rewriteWithPartition() {
        ExactRelation r = SingleRelation.from(vc, getSampleName());
        r.setAlias(getAlias());
        return r;

        // // when a universe sample is used for distinct-count, partitions for
        // subsampling are created based
        // // on the column originally used for the universe hashing.
        // if (param.sampleType.equals("universe")) {
        // Set<String> colNames = vc.getMeta().getColumns(param.sampleTableName());
        // String partitionColName = distinctCountPartitionColumnName();
        // int partitionCount = vc.getDbms().partitionCount();
        //
        // // we will create a new partition column using a hash function, so discard an
        // existing one.
        // List<String> newColNames = new ArrayList<String>();
        // for (String c : colNames) {
        // if (!c.equals(partitionColName)) {
        // newColNames.add(c);
        // }
        // }
        //
        // // a new relation
        // ExactRelation r = SingleRelation.from(vc, getSampleName());
        // r = r.select(Joiner.on(", ").join(newColNames) + ", "
        // + vc.getDbms().modOfHash(param.columnNames.get(0), partitionCount) + " AS " +
        // partitionColName);
        // r.setAlias(getAlias());
        // return r;
        // } else {
        // ExactRelation r = SingleRelation.from(vc, getSampleName());
        // r.setAlias(getAlias());
        // return r;
        // }
    }

    // @Override
    // public ColNameExpr partitionColumn() {
    // String col = partitionColumnName();
    // return new ColNameExpr(col, getTableName().tableName);
    // }

    @Override
    protected List<Expr> samplingProbabilityExprsFor(FuncExpr f) {
        if (f.getFuncName().equals(FuncExpr.FuncName.COUNT_DISTINCT)) {
            if (getSampleType().equals("universe")) {
                return Arrays.<Expr>asList(ConstantExpr.from(vc, getSamplingRatio()));
            } else if (getSampleType().equals("stratified")) {
                return Arrays.<Expr>asList(ConstantExpr.from(vc, 1.0));
            } else if (getSampleType().equals("nosample")) {
                return Arrays.<Expr>asList(ConstantExpr.from(vc, 1.0));
            } else {
                VerdictLogger.warn(this,
                        String.format("%s sample should not be used for count-distinct.", getSampleType()));
                return Arrays.<Expr>asList(ConstantExpr.from(vc, 1.0));
            }
        } else { // SUM, COUNT
            if (!getSampleType().equals("nosample")) {
                String samplingProbCol = samplingProbabilityColumnName();
                return Arrays.<Expr>asList(new ColNameExpr(vc, samplingProbCol, alias));
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
            SampleSizeInfo info = vc.getMeta().getOriginalSizeOf(param.getOriginalTable());
            return (info == null) ? 0 : info.originalTableSize;
        } else {
            SampleSizeInfo info = vc.getMeta().getSampleSizeOf(param);
            if (info == null) {
                return -1;
            }
            return info.sampleSize;
        }
    }

    @Override
    protected List<String> getColumnsOnWhichSamplesAreCreated() {
        return param.getColumnNames();
    }

    /**
     * Using this substitution pattern can handle: 1. user specified his own table
     * alias and using it: no need for substitution since aliases are preserved. 2.
     * user specified his own table alias but referring the raw table name: below
     * pattern handles it. 3. user didn't specified table aliases: below pattern
     * handles it.
     */
    @Override
    protected Map<TableUniqueName, String> tableSubstitution() {
        Map<TableUniqueName, String> s = ImmutableMap.of(param.getOriginalTable(), alias);
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
        s.append(String.format("%s(%s, %s), %s, cost: %f\n", this.getClass().getSimpleName(), getTableName(),
                getAlias(), param.toString(), cost()));
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

    @Override
    public Expr tupleProbabilityColumn() {
        if (!sampleType().equals("nosample")) {
            return new ColNameExpr(vc, samplingProbabilityColumnName(), getAlias());
        } else {
            return new ConstantExpr(vc, 1.0);
        }
    }

    @Override
    public Expr tableSamplingRatio() {
        if (!sampleType().equals("nosample")) {
            double samplingRatio = getSamplingRatio();
            return new ConstantExpr(vc, samplingRatio);
        } else {
            return new ConstantExpr(vc, 1.0);
        }
    }

    @Override
    public List<ColNameExpr> getAssociatedColumnNames(TableNameExpr tabExpr) {
        List<ColNameExpr> colnames = new ArrayList<ColNameExpr>();
        TableUniqueName originalTable = param.getOriginalTable();
        
        if (tabExpr == null) {
            colnames = getAllColumnsofOriginalTable();
        }
        else if (tabExpr.getSchema() == null) {
            if (tabExpr.getTable().equals(originalTable.getTableName()) ||
                tabExpr.getTable().equals(getAlias())) {
                colnames = getAllColumnsofOriginalTable();
            }
        }
        else if (tabExpr.getSchema().equals(originalTable.getSchemaName()) &&
                  (tabExpr.getTable().equals(originalTable.getTableName()) ||
                   tabExpr.getTable().equals(getAlias())) ) {
            colnames = getAllColumnsofOriginalTable();
        }
        
        return colnames;
    }
    
    private List<ColNameExpr> getAllColumnsofOriginalTable() {
        List<ColNameExpr> colnames = new ArrayList<ColNameExpr>();
        TableUniqueName originalTable = param.getOriginalTable();
        Set<String> columns = vc.getMeta().getColumns(originalTable);
        for (String col : columns) {
            colnames.add(new ColNameExpr(vc, col, getAlias()));
        }
        return colnames;
    }
    
}
