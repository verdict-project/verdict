package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.StringManupulations;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsSpark extends Dbms {
	
	private static String DBNAME = "spark";

	protected SQLContext sqlContext;
	
	protected DataFrame df;

	public DbmsSpark(VerdictContext vc, SQLContext sqlContext) throws VerdictException {	
		super(vc, DBNAME);

		this.sqlContext = sqlContext;
	}

	public DataFrame getDatabaseNamesInDataFrame() throws VerdictException {
		DataFrame df = executeSparkQuery("show databases");
		return df;
	}
	
	public void changeDatabase(String schema) throws VerdictException {
		execute(String.format("use %s", schema));
		currentSchema = Optional.fromNullable(schema);
		VerdictLogger.info("Database changed to: " + schema);
	}

	public DataFrame getTablesInDataFrame(String schemaName) throws VerdictException {
		DataFrame df = executeSparkQuery("show tables in " + schemaName);
		return df;
	}
	
	public DataFrame describeTableInDataFrame(TableUniqueName tableUniqueName)  throws VerdictException {
		DataFrame df = executeSparkQuery(String.format("describe %s", tableUniqueName));
		return df;
	}

	@Override
	public boolean execute(String sql) throws VerdictException {
		df = sqlContext.sql(sql);
		return (df.count() > 0)? true : false;
	}

	@Override
	public void executeUpdate(String sql) throws VerdictException {
		execute(sql);
	}

	@Override
	public ResultSet getResultSet() {
		return null;
	}

	@Override
	public DataFrame getDataFrame() {
		return df;
	}

	@Override
	public List<String> getTables(String schema) throws VerdictException {
		List<String> tables = new ArrayList<String>();
		List<Row> rows = executeSparkQuery("show tables in " + schema).collectAsList();
		for (Row row : rows) {
			String table = row.getString(0);
			tables.add(table);
		}
		return tables;
	}

	@Override
	public long getTableSize(TableUniqueName tableName) throws VerdictException {
		String sql = String.format("select count(*) from %s", tableName);
		DataFrame df = executeSparkQuery(sql);
		long size = df.collectAsList().get(0).getLong(0);
		return size;
	}

	@Override
	public List<String> getColumns(TableUniqueName table) throws VerdictException {
		List<String> columns = new ArrayList<String>();
		List<Row> rows = executeSparkQuery("describe " + table).collectAsList();
		for (Row row : rows) {
			String column = row.getString(0);
			columns.add(column);
		}
		return columns;
	}

	@Override
	public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
			throws VerdictException {
		VerdictLogger.warn(this, "deleteEntry() not implemented for DbmsSpark");
	}

	@Override
	public void insertEntry(TableUniqueName tableName, List<String> values) throws VerdictException {
		StringBuilder sql = new StringBuilder(1000);
		sql.append(String.format("insert into %s ", tableName));
		sql.append("select t.* from (select ");
		sql.append(Joiner.on(", ").join(StringManupulations.quoteEveryString(values)));
		sql.append(") t");
		String sql1 = sql.toString();
		System.out.println(sql1);
		executeUpdate(sql1);
//		executeUpdate(sql.toString());
	}

	@Override
	public void createMetaTablesInDMBS(TableUniqueName originalTableName, TableUniqueName sizeTableName,
			TableUniqueName nameTableName) throws VerdictException {
		VerdictLogger.debug(this, "Creates meta tables if not exist.");
		String sql = String.format("CREATE TABLE IF NOT EXISTS %s", sizeTableName)
				+ " (schemaname STRING, "
				+ " tablename STRING, "
				+ " samplesize BIGINT, "
				+ " originaltablesize BIGINT)";
		executeUpdate(sql);
	
		sql = String.format("CREATE TABLE IF NOT EXISTS %s", nameTableName)
				+ " (originalschemaname STRING, "
				+ " originaltablename STRING, "
				+ " sampleschemaaname STRING, "
				+ " sampletablename STRING, "
				+ " sampletype STRING, "
				+ " samplingratio DOUBLE, "
				+ " columnnames STRING)";
		executeUpdate(sql);
		
		VerdictLogger.debug(this, "Meta tables created.");
	}

	@Override
	protected void justCreateUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
		List<String> colNames = vc.getMeta().getColumnNames(param.originalTable);
		
		ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
					            .select("*, count(*) OVER () AS __total_size")
						        .where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", ConstantExpr.from(param.samplingRatio)));
		sampled = sampled.select(
				Joiner.on(", ").join(colNames) +
				", count(*) over () / __total_size" + " AS " + samplingProbCol + ", " +  // attach sampling prob
				randomPartitionColumn());										 // attach partition number
		
		String sql = String.format("create table %s AS ", param.sampleTableName()) + sampled.toSql();
		VerdictLogger.debug(this, "The query used for creating a uniform random sample:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		
		executeUpdate(sql);
	}
	
	@Override
	protected void justCreateStratifiedSampleTableof(SampleParam param) throws VerdictException {
		SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(vc, param.originalTable, "uniform", null, new ArrayList<String>()));
		if (info == null) {
			String msg = "A uniform random must first be created before creating a stratified sample.";
			VerdictLogger.error(this, msg);
			throw new VerdictException(msg);
		}
		
		long originalTableSize = info.originalTableSize;
		double samplingProbability = param.samplingRatio;
		String groupName = Joiner.on(", ").join(param.columnNames);
		String samplingProbColInQuote = quote(vc.getDbms().samplingProbabilityColumnName());
		TableUniqueName sampleTable = param.sampleTableName();
		String allColumns = Joiner.on(", ").join(vc.getMeta().getColumnNames(param.originalTable));
		
		long groupCount = SingleRelation.from(vc, param.originalTable)
									.countDistinctValue(groupName);
		Expr threshold = BinaryOpExpr.from(
				ConstantExpr.from(originalTableSize),
				BinaryOpExpr.from(
						BinaryOpExpr.from(
								ConstantExpr.from(samplingProbability),
								ConstantExpr.from("grp_size"), "/"),
						ConstantExpr.from(groupCount),
						"/"),
				"*");
		ExactRelation sampleWithGrpSize
		  = SingleRelation.from(vc, param.originalTable)
			.select(Arrays.asList("*", String.format("count(*) over (partition by %s) AS grp_size", groupName)))
			.where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", threshold));
		ExactRelation sampleWithSamplingProb
		  = sampleWithGrpSize.select(
				  allColumns + ", "
		          + String.format("count(*) over (partition by %s) / grp_size AS %s", groupName, samplingProbColInQuote) + ", "
		  		  + randomPartitionColumn());
		
		String sql = String.format("CREATE TABLE %s AS ", sampleTable)
					 + sampleWithSamplingProb.toSql();
		VerdictLogger.debug(this, "The query used for creating a stratified sample:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
	}

	@Override
	protected void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException {
		TableUniqueName sampleTableName = param.sampleTableName();
		List<String> colNames = vc.getMeta().getColumnNames(param.originalTable);
		String samplingProbColInQuote = quote(vc.getDbms().samplingProbabilityColumnName());
				
		ExactRelation withSize = SingleRelation.from(vc, param.originalTable)
								 .select("*, count(*) over () AS " + quote("__total_size"));
		ExactRelation sampled = withSize.where(
									modOfHash(param.columnNames.get(0), 1000000) + 
									String.format(" < %.2f", param.samplingRatio*1000000))
					 			.select(Joiner.on(", ").join(colNames)
					 					+ ", count(*) over () / " + quote("__total_size") + " AS " + samplingProbColInQuote + ", "
					 					+ randomPartitionColumn());
		
		String sql = String.format("CREATE TABLE %s AS ", sampleTableName)
				     + sampled.toSql();
		
		VerdictLogger.debug(this, String.format("Creates a table: %s using the following statement:", sampleTableName));
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		this.executeUpdate(sql);
	}
	
	@Override
	public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingNameEntry(param, metaNameTableName);
		insertSampleNameEntryIntoDBMS(param, tempTableName);
		moveTable(tempTableName, metaNameTableName);
	}
	
	protected TableUniqueName createTempTableExlucdingNameEntry(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = Relation.getTempTableName(vc);
		TableUniqueName originalTableName = param.originalTable;
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s "
				+ "WHERE originalschemaname <> \"%s\" OR originaltablename <> \"%s\" OR sampletype <> \"%s\""
				+ "OR samplingratio <> %s OR columnnames <> \"%s\"",
				tempTableName, metaNameTableName, originalTableName.getSchemaName(), originalTableName.getTableName(),
				param.sampleType, samplingRatioToString(param.samplingRatio), columnNameListToString(param.columnNames)));
		return tempTableName;
	}
	
	@Override
	public void updateSampleSizeEntryIntoDBMS(SampleParam param, long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
		insertSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, tempTableName);
		moveTable(tempTableName, metaSizeTableName);
	}

	protected TableUniqueName createTempTableExlucdingSizeEntry(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = Relation.getTempTableName(vc);
		TableUniqueName sampleTableName = param.sampleTableName();
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE schemaname <> \"%s\" OR tablename <> \"%s\" ",
				tempTableName, metaSizeTableName, sampleTableName.getSchemaName(), sampleTableName.getTableName()));
		return tempTableName;
	}

	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
	}

	@Override
	public String modOfHash(String col, int mod) {
		return String.format("crc32(cast(%s as string)) %% %d)", col, mod);
	}
	
	@Override
	public boolean isSpark() {
		return true;
	}

	@Override
	public void close() throws VerdictException {
		// TODO Auto-generated method stub
		
	}
}
