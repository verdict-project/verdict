package edu.umich.verdict.query;

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
import edu.umich.verdict.relation.ApproxSingleRelation;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.FuncExpr;
import edu.umich.verdict.util.StringManipulations;
import edu.umich.verdict.util.VerdictLogger;

public class CreateSampleQuery extends Query {
	
	public CreateSampleQuery(VerdictContext vc, String q) {
		super(vc, q);
	}
	
	@Override
	public void compute() throws VerdictException {
		VerdictSQLParser p = StringManipulations.parserOf(queryString);
		CreateSampleStatementVisitor visitor = new CreateSampleStatementVisitor();
		visitor.visit(p.create_sample_statement());
		
		TableUniqueName tableName = visitor.getTableName();
		TableUniqueName validTableName = (tableName.getSchemaName() != null)? tableName : TableUniqueName.uname(vc, tableName.getTableName());
		Double samplingRatio = visitor.getSamplingRatio();
		String sampleType = visitor.getSampleType();
		List<String> columnNames = visitor.getColumnNames();
		
		buildSamples(new SampleParam(vc, validTableName, sampleType, samplingRatio, columnNames));
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
	protected void buildSamples(SampleParam param) throws VerdictException {
		vc.getDbms().createDatabase(param.sampleTableName().getSchemaName());
		vc.getMeta().refreshDatabases();
		
		if (param.sampleType.equals("uniform")) {
			createUniformRandomSample(param);
		} else if (param.sampleType.equals("universe")) {
			if (param.columnNames.size() == 0) {
				VerdictLogger.error("A column name must be specified for universe samples. Nothing is done.");
			} else {
				if (param.columnNames.size() > 1) {
					VerdictLogger.warn(String.format("Only one column name is expected for universe samples."
							+ " The first column (%s) is used.", param.columnNames.get(0)));
				}
				createUniverseSample(param);
			}
		} else if (param.sampleType.equals("stratified")) {
			if (param.columnNames.size() == 0) {
				VerdictLogger.error("A column name must be specified for stratified samples. Nothing is done.");
			} else {
				if (param.columnNames.size() > 1) {
					VerdictLogger.warn(String.format("Only one column name is supported for stratified samples."
							+ " The first column (%s) is used.", param.columnNames.get(0)));
				}
				createStratifiedSample(param);
			}
		} else {	// recommended
			TableUniqueName originalTable = param.originalTable;
			SampleParam ursParam = new SampleParam(vc, originalTable, "uniform", param.samplingRatio, new ArrayList<String>()); 
			buildSamples(ursParam);		// build a uniform sample
			
			List<Object> aggs = new ArrayList<Object>();
			aggs.add(FuncExpr.count());
			List<String> cnames = new ArrayList<String>(vc.getMeta().getColumns(originalTable));
			for (String c : cnames) {
				aggs.add(FuncExpr.approxCountDistinct(ColNameExpr.from(c), vc));
			}
			List<Object> rs = ApproxSingleRelation.from(vc, ursParam).aggOnSample(aggs).collect().get(0);

			long sampleSize = (Long) rs.get(0);
			int universeCounter = 0;
			int stratifiedCounter = 0;
			for (int i = 1; i < rs.size(); i++) {
				long cd = (Long) rs.get(i);
				String cname = cnames.get(i-1);
				
				if (cd > sampleSize * 0.01 && universeCounter < 10) {
					List<String> sampleOn = new ArrayList<String>();
					sampleOn.add(cname);
					buildSamples(new SampleParam(vc, originalTable, "universe", param.samplingRatio, sampleOn));		// build a universe sample
					universeCounter += 1;
				} else if (stratifiedCounter < 10){
					List<String> sampleOn = new ArrayList<String>();
					sampleOn.add(cname);
					buildSamples(new SampleParam(vc, originalTable, "stratified", param.samplingRatio, sampleOn));		// build a stratified sample
					stratifiedCounter += 1;
				}
			}
		}
		
		vc.getMeta().refreshDatabases();
		vc.getMeta().refreshTables(param.originalTable.getSchemaName());
		vc.getMeta().refreshSampleInfo(param.originalTable.getSchemaName());
	}
	
	protected void createUniformRandomSample(SampleParam param) throws VerdictException {
		VerdictLogger.info(this, String.format("Create a %.2f%% uniform random sample of %s.", param.samplingRatio*100, param.originalTable));
		Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createUniformRandomSampleTableOf(param);
		vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(), sampleAndOriginalSizes.getRight());
	}
	
	protected void createUniverseSample(SampleParam param) throws VerdictException {
		String columnName = param.columnNames.get(0);
		VerdictLogger.info(this, String.format("Create a %.2f%% universe sample of %s on %s.", param.samplingRatio*100, param.originalTable, columnName));
		Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createUniverseSampleTableOf(param);
		vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(), sampleAndOriginalSizes.getRight());
	}
	
	protected void createStratifiedSample(SampleParam param) throws VerdictException {
		String columnName = param.columnNames.get(0);
		VerdictLogger.info(this, String.format("Create a %.2f%% stratified sample of %s on %s.", param.samplingRatio*100, param.originalTable, columnName));
		Pair<Long, Long> sampleAndOriginalSizes = vc.getDbms().createStratifiedSampleTableOf(param);
		vc.getMeta().insertSampleInfo(param, sampleAndOriginalSizes.getLeft(), sampleAndOriginalSizes.getRight());
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
	
	private TableUniqueName tableName;
	
	private Double samplingRatio = 0.01;		// default value is 1%
	
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
