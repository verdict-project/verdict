package edu.umich.verdict.query;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.VerdictSQLBaseVisitor;
import edu.umich.verdict.VerdictSQLLexer;
import edu.umich.verdict.VerdictSQLParser;
import edu.umich.verdict.VerdictSQLParser.Column_nameContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.functions.AggFunction;
import static edu.umich.verdict.relation.functions.AggFunction.*;
import edu.umich.verdict.util.VerdictLogger;

import static edu.umich.verdict.util.NameHelpers.*;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class CreateSampleQuery extends Query {
	
	public CreateSampleQuery(VerdictContext vc, String q) {
		super(vc, q);
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
		} else if (sampleType.equals("universe")) {
			if (columnNames.size() == 0) {
				VerdictLogger.error("A column name must be specified for universe samples. Nothing is done.");
			} else {
				if (columnNames.size() > 1) {
					VerdictLogger.warn(String.format("Only one column name is expected for universe samples."
							+ " The first column (%s) is used.", columnNames.get(0)));
				}
				createUniverseSample(tableName, samplingRatio, columnNames.get(0));
			}
		} else if (sampleType.equals("stratified")) {
			createStratifiedSample(tableName, samplingRatio, columnNames);
		} else {	// recommended
			buildSamples(tableName, samplingRatio, "uniform", columnNames);		// build a uniform sample
			List<Pair<SampleParam, TableUniqueName>> sampleInfo = vc.getMeta().getSampleInfoFor(TableUniqueName.uname(vc, tableName));
			SampleParam param = null;
			TableUniqueName sampleName = null;
			for (Pair<SampleParam, TableUniqueName> e : sampleInfo) {
				param = e.getLeft();
				sampleName = e.getRight();
			}
			
			if (param != null && sampleName != null) {
				TableUniqueName originalTable = TableUniqueName.uname(vc, tableName);
				List<AggFunction> aggs = new ArrayList<AggFunction>();
//				aggs.add(count());
				List<String> cnames = vc.getMeta().getColumnNames(originalTable);
				for (String c : cnames) {
					aggs.add(countDistinct(c));
				}
				List<Object> rs = (new ApproxRelation(vc, originalTable, param)).aggOnSample(aggs).collect().get(0);
				
//				long sampleSize = (Long) rs.get(0);
				for (int i = 0; i < rs.size(); i++) {
					long cd = (Long) rs.get(i);
					String cname = cnames.get(i);
					if (cd > 1000) {
						List<String> sampleOn = new ArrayList<String>();
						sampleOn.add(cname);
						buildSamples(tableName, samplingRatio, "universe", sampleOn);		// build a universe sample
					}
				}
			}
		}
	}
	
	protected void createUniformRandomSample(String tableName, double samplingRatio) throws VerdictException {
		VerdictLogger.info(this, String.format("Create a %.4f%% uniform random sample of %s.", samplingRatio*100, tableName));
		
		Triple<Long, Long, String> sampleAndOriginalSizesAndSampleName = vc.getDbms().createUniformRandomSampleTableOf(tableName, samplingRatio);
		Long sampleSize = sampleAndOriginalSizesAndSampleName.getLeft();
		Long tableSize = sampleAndOriginalSizesAndSampleName.getMiddle();
		String sampleTableName = sampleAndOriginalSizesAndSampleName.getRight();
		
		vc.getMeta().insertSampleInfo(
				schemaOfTableName(vc.getCurrentSchema(), tableName).get(), tableNameOfTableName(tableName), sampleTableName,
				sampleSize, tableSize, "uniform", samplingRatio, new ArrayList<String>());
	}
	
	protected void createUniverseSample(String tableName, double samplingRatio, String columnName) throws VerdictException {
		VerdictLogger.info(this, String.format("Create a %.4f%% universe sample of %s on %s.", samplingRatio*100, tableName, columnName));
		
		Triple<Long, Long, String> sampleAndOriginalSizesAndSampleName = vc.getDbms().createUniverseSampleTableOf(tableName, samplingRatio, columnName);
		Long sampleSize = sampleAndOriginalSizesAndSampleName.getLeft();
		Long tableSize = sampleAndOriginalSizesAndSampleName.getMiddle();
		String sampleTableName = sampleAndOriginalSizesAndSampleName.getRight();
		
		List<String> columnNames = new ArrayList<String>();
		columnNames.add(columnName);
		
		vc.getMeta().insertSampleInfo(
				schemaOfTableName(vc.getCurrentSchema(), tableName).get(), tableNameOfTableName(tableName), sampleTableName,
				sampleSize, tableSize, "universe", samplingRatio, columnNames);
	}
	
	protected void createStratifiedSample(String tableName, double samplingRatio, List<String> columnNames) {
		VerdictLogger.warn(this, "Stratified samples are currently not supported. Skip this process.");
	}
	
	/**
	 * 
	 * @param tableName
	 * @return contains sample infosa s follows:
	 * [first elem] sample type
	 * [second elem] a list of column names on which to build samples
	 */
	protected List<Pair<String, List<String>>> getRecommendedSamples(String tableName) {
		return new ArrayList<Pair<String, List<String>>>();
	}

}


class CreateSampleStatementVisitor extends VerdictSQLBaseVisitor<Void> {
	
	private String tableName;
	
	private Double samplingRatio = 0.01;		// default value is 1%
	
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
	public Void visitCreate_sample_statement(VerdictSQLParser.Create_sample_statementContext ctx) {
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
