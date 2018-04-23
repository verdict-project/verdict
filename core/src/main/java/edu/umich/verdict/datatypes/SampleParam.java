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

package edu.umich.verdict.datatypes;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;

/**
 * Contains sufficient information for creating a sample table (or for pointing to an existing sample table).
 * 
 * @author Yongjoo Park
 *
 */
public class SampleParam implements Serializable, Comparable<SampleParam> {

    private static final long serialVersionUID = 1L;

    // dyoon: we do not need to serialize this.
    private transient VerdictContext vc;

    private TableUniqueName originalTable;

    private String sampleType;

    private Double samplingRatio; // 1.0 = 100%

    private List<String> columnNames;

    public TableUniqueName getOriginalTable() {
        return originalTable;
    }

    public void setOriginalTable(TableUniqueName originalTable) {
        this.originalTable = originalTable;
    }

    public String getSampleType() {
        return sampleType;
    }

    public void setSampleType(String sampleType) {
        this.sampleType = sampleType;
    }

    public Double getSamplingRatio() {
        return samplingRatio;
    }

    public void setSamplingRatio(Double samplingRatio) {
        this.samplingRatio = samplingRatio;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public void setVerdictContext(VerdictContext vc) {
        this.vc = vc;
    }

    public SampleParam(VerdictContext vc, TableUniqueName originalTable, String sampleType, Double samplingRatio,
                       List<String> columnNames) {
        this.vc = vc;
        this.originalTable = originalTable;
        this.sampleType = sampleType;
        this.samplingRatio = samplingRatio;
        if (columnNames == null) {
            this.columnNames = new ArrayList<String>();
        } else {
            this.columnNames = columnNames;
        }
    }

    // Copy constructor
    public SampleParam(SampleParam other) {
        this.vc = other.vc;
        this.originalTable = other.originalTable;
        this.sampleType = other.sampleType;
        this.samplingRatio = other.samplingRatio;
        this.columnNames = new ArrayList<>(other.columnNames);
    }

    public String colNamesInString() {
        return columnNames.isEmpty() ? "N/A" : Joiner.on(",").join(columnNames);
    }

    @Override
    public String toString() {
        return String.format("(%s,%s,%.2f,%s)", originalTable.getTableName(), sampleType, samplingRatio,
                colNamesInString());
    }

    public String toSqlString() {
        String columnString = "";
        if (sampleType.equals("stratified") || sampleType.equals("universe")) {
            columnString = " on " + Joiner.on(",").join(columnNames);
        }
        return String.format("create %.2f%% %s sample of %s%s", samplingRatio * 100, sampleType,
                originalTable.getTableName(), columnString);
    }

    @Override
    public int hashCode() {
        return originalTable.hashCode() + sampleType.hashCode() + samplingRatio.hashCode() + columnNames.hashCode();
    }

    public TableUniqueName sampleTableName() {
        String typeShortName = null;
        if (sampleType.equals("uniform")) {
            typeShortName = "uf";
        } else if (sampleType.equals("universe")) {
            typeShortName = "uv";
        } else if (sampleType.equals("stratified")) {
            typeShortName = "st";
        }

        StringBuilder colNames = new StringBuilder();
        if (columnNames.size() > 0)
            colNames.append("_");
        for (String n : columnNames)
            colNames.append(n);

        return TableUniqueName.uname(vc.getMeta().metaCatalogForDataCatalog(originalTable.getSchemaName()),
                String.format("vs_%s_%s_%s", originalTable.getTableName(), typeShortName,
                        String.format("%.4f", samplingRatio).replace('.', '_'))
                        + ((colNames.length() > 0) ? colNames.toString() : ""));
    }

    @Override
    public boolean equals(Object another) {
        if (another instanceof SampleParam) {
            SampleParam t = (SampleParam) another;
            return originalTable.equals(t.originalTable) && sampleType.equals(t.sampleType)
                    && samplingRatio.equals(t.samplingRatio) && columnNames.equals(t.columnNames);
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(SampleParam o) {
        return this.toString().compareTo(o.toString());
    }
}
