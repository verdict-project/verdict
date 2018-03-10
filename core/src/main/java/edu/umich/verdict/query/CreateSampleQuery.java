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

package edu.umich.verdict.query;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.parser.VerdictSQLBaseVisitor;
import edu.umich.verdict.parser.VerdictSQLParser;
import edu.umich.verdict.parser.VerdictSQLParser.Column_nameContext;
import edu.umich.verdict.relation.ApproxSingleRelation;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Creates samples of a single table.
 * @author Yongjoo Park
 */
public class CreateSampleQuery extends Query {
    
    final private int roughAttributeSizeEstimateInByte = 10;
    
    final private long heuristicSampleSizeInByte = (long) 2e9;      // 2 GB
    
    public CreateSampleQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);
        CreateSampleStatementVisitor visitor = new CreateSampleStatementVisitor();
        visitor.visit(p.create_sample_statement());

        TableUniqueName tableName = visitor.getTableName();
        TableUniqueName validTableName = (tableName.getSchemaName() != null) ? tableName
                : TableUniqueName.uname(vc, tableName.getTableName());
        Double samplingRatio = visitor.getSamplingRatio();
        String sampleType = visitor.getSampleType();
        List<String> columnNames = visitor.getColumnNames();

        buildSamples(new SampleParam(vc, validTableName, sampleType, samplingRatio, columnNames));
    }
    
    protected double heuristicSampleSizeSuggestion(SampleParam param) throws VerdictException {
        VerdictLogger.info(this, "No sampling ratio provided; sample sizes are automatically determined to have at least 1 million tuples.");
        long originalTableSize = vc.getMeta().getTableSize(param.getOriginalTable());
        
        int columnCount = vc.getMeta().getColumns(param.getOriginalTable()).size();
        
        long recommendedSampleSize = Math.min(originalTableSize,
                                              heuristicSampleSizeInByte / columnCount / roughAttributeSizeEstimateInByte);
        double samplingRatio = recommendedSampleSize / (double) originalTableSize;
        return samplingRatio;
    }

    /**
     * 
     * @return A list of objects that contains information for sample creation.
     *         [first] type: String, info: table name from which to create a sample
     *         [second] type: Double, info: sampling ratio [third] type: String,
     *         info: sample type (uniform, universal, stratified, automatic)
     *         [fourth] type: List<String>, info: a list of column names. for
     *         universal sampling, this is a list of length one. for stratified
     *         sampling, this is a list of (possibly) multiple column names.
     * @throws VerdictException
     */
    protected void buildSamples(SampleParam param) throws VerdictException {
        String schemaName = param.sampleTableName().getSchemaName();
        if (!vc.getMeta().getDatabases().contains(schemaName)) {
            vc.getDbms().createDatabase(schemaName);
        }
        vc.getMeta().refreshDatabases();
        
        // we decide the sample size (in ratio) if it's not specified in the "create sample" clause.
        if (param.getSamplingRatio() < 0) {
            double samplingRatio = heuristicSampleSizeSuggestion(param);
            param.setSamplingRatio(samplingRatio);
        }

        if (param.getSampleType().equals("uniform")) {
            createUniformRandomSample(param);
        } else if (param.getSampleType().equals("universe")) {
            if (param.getColumnNames().size() == 0) {
                VerdictLogger.error("A column name must be specified for universe samples. Nothing is done.");
            } else {
                createUniverseSample(param);
            }
        } else if (param.getSampleType().equals("stratified")) {
            if (param.getColumnNames().size() == 0) {
                VerdictLogger.error("A column name must be specified for stratified samples. Nothing is done.");
            } else {
                createStratifiedSample(param);
            }
        } else { // without specific options, recommended
            TableUniqueName originalTable = param.getOriginalTable();
            SampleParam ursParam = new SampleParam(vc, originalTable, "uniform", param.getSamplingRatio(),
                    new ArrayList<String>());
            buildSamples(ursParam); // build a uniform sample

            // check the number of unique attribute values in each column. Based on this information, we
            // decide to build an universe sample or a stratified sample for the column.
            List<Object> aggs = new ArrayList<Object>();
            aggs.add(FuncExpr.count());
            List<String> cnames = new ArrayList<String>(vc.getMeta().getColumns(originalTable));
            for (String c : cnames) {
                aggs.add(FuncExpr.approxCountDistinct(vc, ColNameExpr.from(vc, c)));
            }
            List<Object> rs = ApproxSingleRelation.from(vc, ursParam).aggOnSample(aggs).collect().get(0);

            long sampleSize = (Long) rs.get(0);
            int universeCounter = 0;
            int stratifiedCounter = 0;
            for (int i = 1; i < rs.size(); i++) {
                long cd = (Long) rs.get(i);     // count-distinct
                String cname = cnames.get(i - 1);

                if (cd > sampleSize * 0.01 && universeCounter < 10) {
                    List<String> sampleOn = new ArrayList<String>();
                    sampleOn.add(cname);
                    // build a universe sample
                    buildSamples(new SampleParam(vc, originalTable, "universe", param.getSamplingRatio(), sampleOn));
                    universeCounter += 1;
                } else if (stratifiedCounter < 10) {
                    List<String> sampleOn = new ArrayList<String>();
                    sampleOn.add(cname);
                    // build a stratified sample
                    buildSamples(new SampleParam(vc, originalTable, "stratified", param.getSamplingRatio(), sampleOn));
                    stratifiedCounter += 1;
                }
            }
        }
        
        // refresh meta data
        vc.getMeta().refreshDatabases();
        vc.getMeta().refreshTables(param.getOriginalTable().getSchemaName());
        vc.getMeta().refreshSampleInfo(param.getOriginalTable().getSchemaName(), true);
    }

    protected void createUniformRandomSample(SampleParam param) throws VerdictException {
        VerdictLogger.info(this, String.format("Creates a %.2f%% uniform random sample of %s.",
                param.getSamplingRatio() * 100, param.getOriginalTable()));
        Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createUniformRandomSampleTableOf(param);
        vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(), sampleAndOriginalSizes.getRight());
    }

    protected void createUniverseSample(SampleParam param) throws VerdictException {
//        String columnName = param.getColumnNames().get(0);
        VerdictLogger.info(this, String.format("Creates a %.2f%% universe sample of %s on %s.",
                param.getSamplingRatio() * 100, param.getOriginalTable(), param.colNamesInString()));
        Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createUniverseSampleTableOf(param);
        vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(), sampleAndOriginalSizes.getRight());
    }

    protected void createStratifiedSample(SampleParam param) throws VerdictException {
//        String columnName = param.getColumnNames().get(0);
        VerdictLogger.info(this, String.format("Creates a %.2f%% stratified sample of %s on %s.",
                param.getSamplingRatio() * 100, param.getOriginalTable(), param.colNamesInString()));
        Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createStratifiedSampleTableOf(param);
        if (sampleAndOriginalSizes == null) {
            // Let's create a uniform sample first (with heuristic sampling ratio)
            // since it does not exist.
            double heuristicRatio = this.heuristicSampleSizeSuggestion(param);
            SampleParam uniformSample = new SampleParam(param);
            uniformSample.setSamplingRatio(heuristicRatio);
            uniformSample.setSampleType("uniform");
            uniformSample.setColumnNames(new ArrayList<String>());
            VerdictLogger.info(this, String.format("Creating a uniform sample first."));
            createUniformRandomSample(uniformSample);
            sampleAndOriginalSizes = vc.getDbms().createUniformRandomSampleTableOf(uniformSample);
            vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(),
                sampleAndOriginalSizes.getRight());

            // refresh sample tables & info before creating the stratified sample.
            vc.getMeta().refreshDatabases();
            vc.getMeta().refreshTables(uniformSample.getOriginalTable().getSchemaName());
            vc.getMeta().refreshSampleInfo(uniformSample.getOriginalTable().getSchemaName(), true);

            // now create a stratified sample.
            sampleAndOriginalSizes = vc.getDbms().createStratifiedSampleTableOf(param);
        }
        vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(),
            sampleAndOriginalSizes.getRight());
    }

    /**
     * 
     * @param tableName
     * @return contains sample infosa s follows: [first elem] sample type [second
     *         elem] a list of column names on which to build samples
     */
    protected List<Pair<String, List<String>>> getRecommendedSamples(String tableName) {
        return new ArrayList<Pair<String, List<String>>>();
    }

}

class CreateSampleStatementVisitor extends VerdictSQLBaseVisitor<Void> {

    private TableUniqueName tableName;

    private Double samplingRatio = -1.0;        // use a negative value for "not specified"

    private String sampleType = "recommended";

    private List<String> columnNames = new ArrayList<String>();

    public TableUniqueName getTableName() {
        return tableName;
    }

    public double getSamplingRatio() {
        return samplingRatio;
    }

    public String getSampleType() {
        return sampleType;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public Void visitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx) {
        if (ctx.size != null) {
            samplingRatio = 0.01 * Double.valueOf(ctx.size.getText());
        }
        visitChildren(ctx);
        return null;
    }

    @Override
    public Void visitTable_name(VerdictSQLParser.Table_nameContext ctx) {
        String schema = null;
        if (ctx.schema != null) {
            schema = ctx.schema.getText();
        }
        String table = ctx.table.getText();

        tableName = TableUniqueName.uname(schema, table);
        return null;
    }

    @Override
    public Void visitSample_type(VerdictSQLParser.Sample_typeContext ctx) {
        if (ctx.UNIFORM() != null) {
            sampleType = "uniform";
        } else if (ctx.UNIVERSE() != null) {
            sampleType = "universe";
        } else if (ctx.STRATIFIED() != null) {
            sampleType = "stratified";
        }
        return null;
    }

    @Override
    public Void visitOn_columns(VerdictSQLParser.On_columnsContext ctx) {
        for (Column_nameContext c : ctx.column_name()) {
            columnNames.add(c.getText());
        }
        return null;
    }

}
