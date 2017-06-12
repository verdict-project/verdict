package edu.umich.verdict.query;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;

public class DropSampleQuery extends Query {

	public DropSampleQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public DropSampleQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		DeleteSampleStatementVisitor visitor = new DeleteSampleStatementVisitor();
		visitor.visit(p.delete_sample_statement());
		
		String tableName = visitor.getTableName();
		Double samplingRatio = visitor.getSamplingRatio();
		String sampleType = visitor.getSampleType();
		List<String> columnNames = visitor.getColumnNames();
		
		deleteSampleOf(tableName, samplingRatio, sampleType, columnNames);
		return null;
	}
	
	protected void deleteSampleOf(String tableName, double samplingRatio, String sampleType, List<String> columnNames) throws VerdictException {
		List<Pair<SampleParam, TableUniqueName>> sampleParamAndTableName = vc.getMeta().getSampleInfoFor(TableUniqueName.uname(vc, tableName));
		
		for (Pair<SampleParam, TableUniqueName> e : sampleParamAndTableName) {
			SampleParam param = e.getLeft();
			TableUniqueName sampleName = e.getRight();
			if (isSamplingRatioEqual(param.samplingRatio, samplingRatio)
					&& isSampleTypeEqual(param.sampleType, sampleType)
					&& param.columnNames.equals(columnNames)) {
				vc.getDbms().dropTable(sampleName);
				vc.getMeta().deleteSampleInfo(param.originalTable, param.sampleType, param.samplingRatio, columnNames);
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
		if (t1.equals("automatic") || t2.equals("automatic")) {
			return true;
		} else {
			return t1.equals(t2);
		}
	}

}


class DeleteSampleStatementVisitor extends VerdictSQLBaseVisitor<Void> {
	
	private String tableName;
	
	private Double samplingRatio = -1.0;	// negative value is for delete everything
	
	private String sampleType = "automatic";
	
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
		tableName = ctx.table_name().getText();
		if (ctx.size != null) {
			samplingRatio = 0.01 * Double.valueOf(ctx.size.getText());
		}
		visit(ctx.sample_type());
		return null;
	}
	
	@Override
	public Void visitUniform_random_sample(VerdictSQLParser.Uniform_random_sampleContext ctx) {
		sampleType = "uniform";
		return null;
	}
	
	@Override
	public Void visitUniversal_sample(VerdictSQLParser.Universal_sampleContext ctx) {
		sampleType = "universal";
		columnNames.add(ctx.column_name().getText());
		return null;
	}
	
	@Override
	public Void visitStratified_sample(VerdictSQLParser.Stratified_sampleContext ctx) {
		sampleType = "stratified";
		for (VerdictSQLParser.Column_nameContext name : ctx.column_name()) {
			columnNames.add(name.getText());
		}
		return null;
	}
	
	@Override
	public Void visitAutomatic_sample(VerdictSQLParser.Automatic_sampleContext ctx) {
		sampleType = "automatic";
		return null;
	}
	
}
