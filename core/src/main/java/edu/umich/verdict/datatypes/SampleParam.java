package edu.umich.verdict.datatypes;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictJDBCContext;


public class SampleParam implements Comparable<SampleParam> {

	private VerdictContext vc;
	
	public TableUniqueName originalTable;
	
	public String sampleType;
	
	public Double samplingRatio;
	
	public List<String> columnNames;

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

	public SampleParam(VerdictContext vc, TableUniqueName originalTable, String sampleType, Double samplingRatio, List<String> columnNames) {
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
	
	public String colNamesInString() {
		return Joiner.on(",").join(columnNames);
	}
	
	@Override
	public String toString() {
		return String.format("(%s,%s,%.2f,%s)", originalTable.getTableName(), sampleType, samplingRatio, colNamesInString());
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
		if (columnNames.size() > 0) colNames.append("_");
		for (String n : columnNames) colNames.append(n);
		
		return TableUniqueName.uname(
				vc.getMeta().metaCatalogForDataCatalog(originalTable.getSchemaName()),
				String.format("vs_%s_%s_%s", originalTable.getTableName(), typeShortName,
							  String.format("%.4f", samplingRatio).replace('.', '_'))
				 + ((colNames.length() > 0)? colNames.toString() : ""));
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
		return originalTable.compareTo(o.originalTable);
	}
}
