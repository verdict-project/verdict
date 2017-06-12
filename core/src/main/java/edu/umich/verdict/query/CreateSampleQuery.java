package edu.umich.verdict.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.VerdictLogger;

import static edu.umich.verdict.util.NameHelpers.*;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class CreateSampleQuery extends Query {
	
	public CreateSampleQuery(String q, VerdictContext vc) {
		super(q, vc);
	}
	
	public CreateSampleQuery(Query parent) {
		super(parent.queryString, parent.vc);
	}
	
	@Override
	public ResultSet compute() throws VerdictException {
		VerdictSQLLexer l = new VerdictSQLLexer(CharStreams.fromString(queryString));
		VerdictSQLParser p = new VerdictSQLParser(new CommonTokenStream(l));
		CreateSampleStatementVisitor visitor = new CreateSampleStatementVisitor();
		visitor.visit(p.create_sample_statement());
		
		String tableName = visitor.getTableName();
		Double samplingRatio = visitor.getSamplingRatio();
		String sampleType = visitor.getSampleType();
		List<String> columnNames = visitor.getColumnNames();
		
		buildSamples(tableName, samplingRatio, sampleType, columnNames);
		
		return null;
	}
	
	/**
	 * 
	 * @return A list of objects that contains information for sample creation.
	 * [first] type: String, info: table name from which to create a sample
	 * [second] type: Double, info: sampling ratio
	 * [third] type: String, info: sample type (uniform, universal, stratified, automatic)
	 * [fourth] type: List<String>, info: a list of column names. for universal sampling, this is a list of length one.
	 * 		for stratified sampling, this is a list of (possibly) multiple column names.
	 * @throws VerdictException 
	 */
	protected void buildSamples(String tableName, double samplingRatio, String sampleType, List<String> columnNames) throws VerdictException {
		if (sampleType.equals("uniform")) {
			createUniformRandomSample(tableName, samplingRatio);
		} else if (sampleType.equals("universal")) {
			createUniversalSample(tableName, samplingRatio, columnNames.get(0));
		} else if (sampleType.equals("stratified")) {
			createStratifiedSample(tableName, samplingRatio, columnNames);
		} else {	// automatic
			List<Pair<String, List<String>>> sampleInfos = getSuggestedSamples(tableName);
			for (Pair<String, List<String>> e : sampleInfos) {
				String aSampleType = e.getLeft();
				List<String> aColumnNames = e.getRight();
				if (aSampleType.equals("auto")) continue;		// suggested samples must not include any auto type
				buildSamples(tableName, samplingRatio, aSampleType, aColumnNames);
			}
		}
	}
	
	protected void createUniformRandomSample(String tableName, double samplingRatio) throws VerdictException {
		VerdictLogger.info(this, String.format("Create a %.4f uniform random sample of %s.", samplingRatio*100, tableName));
		
		Triple<Long, Long, String> sampleAndOriginalSizesAndSampleName = vc.getDbms().createUniformRandomSampleTableOf(tableName, samplingRatio);
		Long sampleSize = sampleAndOriginalSizesAndSampleName.getLeft();
		Long tableSize = sampleAndOriginalSizesAndSampleName.getMiddle();
		String sampleTableName = sampleAndOriginalSizesAndSampleName.getRight();
		
		vc.getMeta().insertSampleInfo(
				schemaOfTableName(vc.getCurrentSchema(), tableName).get(), tableNameOfTableName(tableName), sampleTableName,
				sampleSize, tableSize, "uniform", samplingRatio, new ArrayList<String>());
	}
	
	protected void createUniversalSample(String tableName, double samplingRatio, String columnName) throws VerdictException {
		VerdictLogger.info(this, String.format("Create a %.4f universal sample of %s.", samplingRatio*100, tableName));
		
		Triple<Long, Long, String> sampleAndOriginalSizesAndSampleName = vc.getDbms().createUniversalSampleTableOf(tableName, samplingRatio, columnName);
		Long sampleSize = sampleAndOriginalSizesAndSampleName.getLeft();
		Long tableSize = sampleAndOriginalSizesAndSampleName.getMiddle();
		String sampleTableName = sampleAndOriginalSizesAndSampleName.getRight();
		
		List<String> columnNames = new ArrayList<String>();
		columnNames.add(columnName);
		
		vc.getMeta().insertSampleInfo(
				schemaOfTableName(vc.getCurrentSchema(), tableName).get(), tableNameOfTableName(tableName), sampleTableName,
				sampleSize, tableSize, "universal", samplingRatio, columnNames);
	}
	
	protected void createStratifiedSample(String tableName, double samplingRatio, List<String> columnNames) {
		VerdictLogger.warn(this, "Stratified samples are not supported. Skip this process.");
	}
	
	/**
	 * 
	 * @param tableName
	 * @return contains sample infosa s follows:
	 * [first elem] sample type
	 * [second elem] a list of column names on which to build samples
	 */
	protected List<Pair<String, List<String>>> getSuggestedSamples(String tableName) {
		return new ArrayList<Pair<String, List<String>>>();
	}

}


class CreateSampleStatementVisitor extends VerdictSQLBaseVisitor<Void> {
	
	private String tableName;
	
	private Double samplingRatio = 0.01;		// default value is 1%
	
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
	public Void visitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx) {
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
