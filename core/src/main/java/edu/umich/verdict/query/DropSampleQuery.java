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
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class DropSampleQuery extends Query {

    public DropSampleQuery(VerdictContext vc, String q) {
        super(vc, q);
    }

    @Override
    public void compute() throws VerdictException {
        VerdictSQLParser p = StringManipulations.parserOf(queryString);
        DeleteSampleStatementVisitor visitor = new DeleteSampleStatementVisitor();
        visitor.visit(p.delete_sample_statement());

        TableUniqueName tableName = visitor.getTableName();
        Double samplingRatio = visitor.getSamplingRatio();
        String sampleType = visitor.getSampleType();
        List<String> columnNames = visitor.getColumnNames();
        TableUniqueName effectiveTableName = (tableName.getSchemaName() != null) ? tableName
                : ((vc.getCurrentSchema().isPresent()) ? TableUniqueName.uname(vc, tableName.getTableName()) : null);
        if (effectiveTableName == null) {
            VerdictLogger.error("No table is specified; Verdict doesn't do anything.");
            return;
        }

        deleteSampleOf(effectiveTableName, samplingRatio, sampleType, columnNames);
        vc.getMeta().refreshSampleInfo(effectiveTableName.getSchemaName());
    }

    protected void deleteSampleOf(TableUniqueName tableName, double samplingRatio, String sampleType,
            List<String> columnNames) throws VerdictException {
        List<Pair<SampleParam, TableUniqueName>> sampleParamAndTableName = vc.getMeta().getSampleInfoFor(tableName);

        for (Pair<SampleParam, TableUniqueName> e : sampleParamAndTableName) {
            SampleParam param = e.getLeft();
            TableUniqueName sampleName = e.getRight();
            if (isSamplingRatioEqual(param.getSamplingRatio(), samplingRatio)
                    && isSampleTypeEqual(param.getSampleType(), sampleType)
                    && isColumnNamesEqual(param.getColumnNames(), columnNames)) {
                vc.getDbms().dropTable(sampleName);
                vc.getMeta().deleteSampleInfo(param);
                VerdictLogger.info(String.format("Deleted a sample table %s and its meta information.", sampleName));
            }
        }
    }

    private boolean isSamplingRatioEqual(double r1, double r2) {
        if (r1 < 0 || r2 < 0) {
            return true;
        } else {
            return r1 == r2;
        }
    }

    private boolean isSampleTypeEqual(String t1, String t2) {
        if (t1.equals("recommended") || t2.equals("recommended")) {
            return true;
        } else {
            return t1.equals(t2);
        }
    }

    private boolean isColumnNamesEqual(List<String> colNamesOfSample, List<String> colNamesInStatement) {
        if (colNamesInStatement.size() == 0) {
            return true;
        } else if (colNamesOfSample.equals(colNamesInStatement)) {
            return true;
        } else {
            return false;
        }
    }

}

class DeleteSampleStatementVisitor extends VerdictSQLBaseVisitor<Void> {

    private TableUniqueName tableName;

    private Double samplingRatio = -1.0; // negative value is for delete everything

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
    public Void visitDelete_sample_statement(VerdictSQLParser.Delete_sample_statementContext ctx) {
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
