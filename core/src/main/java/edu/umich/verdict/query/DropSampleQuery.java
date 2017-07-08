package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.VerdictSQLParser.Column_nameContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.StringManupulations;
import edu.umich.verdict.util.VerdictLogger;

public class DropSampleQuery extends Query {

	public DropSampleQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLParser p = StringManupulations.parserOf(queryString);
		DeleteSampleStatementVisitor visitor = new DeleteSampleStatementVisitor();
		visitor.visit(p.delete_sample_statement());
		
		String tableName = visitor.getTableName();
		Double samplingRatio = visitor.getSamplingRatio();
		String sampleType = visitor.getSampleType();
		List<String> columnNames = visitor.getColumnNames();
		
		deleteSampleOf(tableName, samplingRatio, sampleType, columnNames);
		vc.getMeta().refreshSampleInfo(vc.getCurrentSchema().get());
		return null;
	}
	
	protected void deleteSampleOf(String tableName, double samplingRatio, String sampleType, List<String> columnNames) throws VerdictException {
		List<Pair<SampleParam, TableUniqueName>> sampleParamAndTableName = vc.getMeta().getSampleInfoFor(TableUniqueName.uname(vc, tableName));
		
		for (Pair<SampleParam, TableUniqueName> e : sampleParamAndTableName) {
			SampleParam param = e.getLeft();
			TableUniqueName sampleName = e.getRight();
			if (isSamplingRatioEqual(param.samplingRatio, samplingRatio)
					&& isSampleTypeEqual(param.sampleType, sampleType)
					&& isColumnNamesEqual(param.columnNames, columnNames)) {
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
	
	private String tableName;
	
	private Double samplingRatio = -1.0;	// negative value is for delete everything
	
	private String sampleType = "recommended";
	
	private List<String> columnNames = new ArrayList<String>();
	
	public String getTableName() {
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
		tableName = ctx.getText();
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
