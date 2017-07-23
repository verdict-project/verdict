package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import edu.umich.verdict.util.StringManipulations;
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
        return (df != null)? true : false;
		//return (df.count() > 0)? true : false;
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
	
	public DataFrame emptyDataFrame() {
		return sqlContext.emptyDataFrame();
	}

	@Override
	public Set<String> getDatabases() throws VerdictException {
		Set<String> databases = new HashSet<String>();
		List<Row> rows = executeSparkQuery("show databases").collectAsList();
		for (Row row : rows) {
			String dbname = row.getString(0); 
			databases.add(dbname);
		}
		return databases;
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
	public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
		Map<String, String> col2type = new LinkedHashMap<String, String>();
		List<Row> rows = executeSparkQuery("describe " + table).collectAsList();
		for (Row row : rows) {
			String column = row.getString(0);
			String type = row.getString(2);
			col2type.put(column, type);
		}
		return col2type;
	}

	@Override
	public void deleteEntry(TableUniqueName tableName, List<Pair<String, String>> colAndValues)
			throws VerdictException {
		VerdictLogger.warn(this, "deleteEntry() not implemented for DbmsSpark");
	}

	@Override
	public void insertEntry(TableUniqueName tableName, List<Object> values) throws VerdictException {
		StringBuilder sql = new StringBuilder(1000);
		sql.append(String.format("insert into %s ", tableName));
		sql.append("select t.* from (select ");
		String with = "'";
		sql.append(Joiner.on(", ").join(StringManipulations.quoteString(values, with)));
		sql.append(") t");
		executeUpdate(sql.toString());
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
		TableUniqueName temp = createUniformRandomSampledTable(param);
		attachUniformProbabilityToTempTable(param, temp);
//		createUniformRandomSampleFromRandTable(param, temp);
		dropTable(temp);
		
//		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
//		Set<String> colNames = vc.getMeta().getColumns(param.originalTable);
//		
//		ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
//					            .select("*, count(*) OVER () AS __total_size")
//						        .where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", ConstantExpr.from(param.samplingRatio)));
//		sampled = sampled.select(
//				Joiner.on(", ").join(colNames) +
//				", count(*) over () / __total_size" + " AS " + samplingProbCol + ", " +  // attach sampling prob
//				randomPartitionColumn());										 // attach partition number
//		
//		String sql = String.format("create table %s AS ", param.sampleTableName()) + sampled.toSql();
//		VerdictLogger.debug(this, "The query used for creating a uniform random sample:");
//		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
//		
//		executeUpdate(sql);
	}
	
	private TableUniqueName createUniformRandomSampledTable(SampleParam param) throws VerdictException {
		String whereClause = String.format("rand(unix_timestamp()) < %f", param.samplingRatio);
		ExactRelation sampled = SingleRelation.from(vc, param.getOriginalTable())
				                .where(whereClause)
				                .select("*, " + randomPartitionColumn());
		TableUniqueName temp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		
		String sql = String.format("create table %s as %s", temp, sampled.toSql());
		VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
		return temp;
	}
	
	private void attachUniformProbabilityToTempTable(SampleParam param, TableUniqueName temp) throws VerdictException {
		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
		long total_size = SingleRelation.from(vc, param.getOriginalTable()).countValue();
		long sample_size = SingleRelation.from(vc, temp).countValue();
		
		ExactRelation withRand = SingleRelation.from(vc, temp)
		                         .select("*, " + String.format("%d / %d as %s", sample_size, total_size, samplingProbCol));
		String sql = String.format("create table %s as %s", param.sampleTableName(), withRand.toSql());
		VerdictLogger.debug(this, "The query used for creating a temporary table without sampling probabilities:");
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
		
		TableUniqueName groupSizeTemp = createGroupSizeTempTable(param);
		createStratifiedSampleFromGroupSizeTemp(param, groupSizeTemp);
		dropTable(groupSizeTemp);
		
//		long originalTableSize = info.originalTableSize;
//		double samplingProbability = param.samplingRatio;
//		String groupName = Joiner.on(", ").join(param.columnNames);
//		String samplingProbColInQuote = quote(vc.getDbms().samplingProbabilityColumnName());
//		TableUniqueName sampleTable = param.sampleTableName();
//		String allColumns = Joiner.on(", ").join(vc.getMeta().getColumns(param.originalTable));
//		
//		long groupCount = SingleRelation.from(vc, param.originalTable)
//									.countDistinctValue(groupName);
//		Expr threshold = BinaryOpExpr.from(
//				ConstantExpr.from(originalTableSize),
//				BinaryOpExpr.from(
//						BinaryOpExpr.from(
//								ConstantExpr.from(samplingProbability),
//								ConstantExpr.from("grp_size"), "/"),
//						ConstantExpr.from(groupCount),
//						"/"),
//				"*");
//		ExactRelation sampleWithGrpSize
//		  = SingleRelation.from(vc, param.originalTable)
//			.select(Arrays.asList("*", String.format("count(*) over (partition by %s) AS grp_size", groupName)))
//			.where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", threshold));
//		ExactRelation sampleWithSamplingProb
//		  = sampleWithGrpSize.select(
//				  allColumns + ", "
//		          + String.format("count(*) over (partition by %s) / grp_size AS %s", groupName, samplingProbColInQuote) + ", "
//		  		  + randomPartitionColumn());
//		
//		String sql = String.format("CREATE TABLE %s AS ", sampleTable)
//					 + sampleWithSamplingProb.toSql();
//		VerdictLogger.debug(this, "The query used for creating a stratified sample:");
//		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
//		executeUpdate(sql);
	}
	
	private TableUniqueName createGroupSizeTempTable(SampleParam param) throws VerdictException {
		TableUniqueName groupSizeTemp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		ExactRelation groupSize = SingleRelation.from(vc, param.originalTable)
				                  .groupby(param.columnNames)
				                  .agg("count(*) AS __group_size");
		String sql = String.format("create table %s as %s", groupSizeTemp, groupSize.toSql());
		VerdictLogger.debug(this, "The query used for the group-size temp table: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
		return groupSizeTemp;
	}
	
	final private long NULL_LONG = Long.MIN_VALUE + 1;
	
	final private String NULL_STRING = "VERDICT_NULL"; 
	
	private void createStratifiedSampleFromGroupSizeTemp(SampleParam param, TableUniqueName groupSizeTemp) throws VerdictException {
		Map<String, String> col2types = vc.getMeta().getColumn2Types(param.originalTable);
		SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(vc, param.originalTable, "uniform", null, new ArrayList<String>()));
		long originalTableSize = info.originalTableSize;
		long groupCount = SingleRelation.from(vc, groupSizeTemp).countValue();
		String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();
		
		// equijoin expression that considers possible null values
		List<Pair<Expr, Expr>> joinExprs = new ArrayList<Pair<Expr, Expr>>();
		for (String col : param.getColumnNames()) {
			boolean isString = false;
			
			if (col2types.containsKey(col)
				&& (col2types.get(col).toLowerCase().contains("char")
				    || col2types.get(col).toLowerCase().contains("str"))) {
				isString = true;
			}
			
			if (isString) {
				Expr left = Expr.from(String.format("case when s.%s is null then '%s' else s.%s end", col, NULL_STRING, col));
				Expr right = Expr.from(String.format("case when t.%s is null then '%s' else t.%s end", col, NULL_STRING, col));
				joinExprs.add(Pair.of(left, right));
			} else {
				Expr left = Expr.from(String.format("case when s.%s is null then %d else s.%s end", col, NULL_LONG, col));
				Expr right = Expr.from(String.format("case when t.%s is null then %d else t.%s end", col, NULL_LONG, col));
				joinExprs.add(Pair.of(left, right));
			}
		}
		
		// where clause using rand function
		String whereClause = String.format("rand(unix_timestamp()) < %d * %f / %d / __group_size",
		                                   originalTableSize,
		                                   param.getSamplingRatio(),
                                           groupCount);
		
		// aliased select list
		List<String> selectElems = new ArrayList<String>();
		for (String col : col2types.keySet()) {
			selectElems.add(String.format("s.%s", col));
		}
		
		// sample table
		TableUniqueName sampledNoRand = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		ExactRelation sampled = SingleRelation.from(vc, param.getOriginalTable()).withAlias("s")
				                .join(SingleRelation.from(vc, groupSizeTemp).withAlias("t"), joinExprs)
				                .where(whereClause)
				                .select(Joiner.on(", ").join(selectElems) + ", __group_size");
		String sql1 = String.format("create table %s as %s", sampledNoRand, sampled.toSql());
		VerdictLogger.debug(this, "The query used for creating a stratified sample without sampling probabilities.");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql1), "  ");
		executeUpdate(sql1);
		
		// attach sampling probabilities and random partition number
		ExactRelation sampledGroupSize = SingleRelation.from(vc, sampledNoRand)
                                         .groupby(param.columnNames)
       				                     .agg("count(*) AS __group_size_in_sample");
		ExactRelation withRand = SingleRelation.from(vc, sampledNoRand).withAlias("s")
				                 .join(sampledGroupSize.withAlias("t"), joinExprs)
				                 .select(Joiner.on(", ").join(selectElems)
				                		     + String.format(", __group_size_in_sample  / __group_size as %s", samplingProbColName)
				                		     + ", " + randomPartitionColumn());
		String sql2 = String.format("create table %s as %s", param.sampleTableName(), withRand.toSql());
		VerdictLogger.debug(this, "The query used for creating a stratified sample with sampling probabilities.");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql2), "  ");
		executeUpdate(sql2);
		
		dropTable(sampledNoRand);
	}

	@Override
	protected void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException {
		TableUniqueName temp = createUniverseSampledTable(param);
		createUniverseSampleWithProbFromSample(param, temp);
		dropTable(temp);
		
//		TableUniqueName sampleTableName = param.sampleTableName();
//		Set<String> colNames = vc.getMeta().getColumns(param.originalTable);
//		String samplingProbColInQuote = quote(vc.getDbms().samplingProbabilityColumnName());
//				
//		ExactRelation withSize = SingleRelation.from(vc, param.originalTable)
//								 .select("*, count(*) over () AS " + quote("__total_size"));
//		ExactRelation sampled = withSize.where(
//									modOfHash(param.columnNames.get(0), 1000000) + 
//									String.format(" < %.2f", param.samplingRatio*1000000))
//					 			.select(Joiner.on(", ").join(colNames)
//					 					+ ", count(*) over () / " + quote("__total_size") + " AS " + samplingProbColInQuote + ", "
//					 					+ randomPartitionColumn());
//		
//		String sql = String.format("CREATE TABLE %s AS ", sampleTableName)
//				     + sampled.toSql();
//		
//		VerdictLogger.debug(this, String.format("Creates a table: %s using the following statement:", sampleTableName));
//		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
//		this.executeUpdate(sql);
	}
	
	private TableUniqueName createUniverseSampledTable(SampleParam param) throws VerdictException {
		TableUniqueName temp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
				                .where(modOfHash(param.columnNames.get(0), 1000000) + 
				                		   String.format(" < %.2f", param.samplingRatio * 1000000));
		String sql = String.format("create table %s AS %s", temp, sampled.toSql());
		VerdictLogger.debug(this, "The query used for creating a universe sample without sampling probability:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
		return temp;
	}
	
	private void createUniverseSampleWithProbFromSample(SampleParam param, TableUniqueName temp) throws VerdictException {
		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
		ExactRelation sampled = SingleRelation.from(vc, temp);
		long total_size = SingleRelation.from(vc, param.originalTable).countValue();
		long sample_size = sampled.countValue();
		
		ExactRelation withProb = sampled.select(
									String.format("*, %d / %d AS %s", sample_size, total_size, samplingProbCol) + ", " +
									randomPartitionColumn());
		String sql = String.format("create table %s AS %s", param.sampleTableName(), withProb.toSql());
		VerdictLogger.debug(this, "The query used for creating a universe sample with sampling probability:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
	}
	
	@Override
	public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingNameEntry(param, metaNameTableName);
		insertSampleNameEntryIntoDBMS(param, tempTableName);
		moveTable(tempTableName, metaNameTableName);
	}
	
	protected TableUniqueName createTempTableExlucdingNameEntry(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		String metaSchema = param.sampleTableName().getSchemaName();
		TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
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
		String metaSchema = param.sampleTableName().getSchemaName();
		TableUniqueName tempTableName = Relation.getTempTableName(vc, metaSchema);
		TableUniqueName sampleTableName = param.sampleTableName();
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE schemaname <> \"%s\" OR tablename <> \"%s\" ",
				tempTableName, metaSizeTableName, sampleTableName.getSchemaName(), sampleTableName.getTableName()));
		return tempTableName;
	}
	
	@Override
	public void deleteSampleNameEntryFromDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTable = createTempTableExlucdingNameEntry(param, metaNameTableName);
		moveTable(tempTable, metaNameTableName);
	}
	
	@Override
	public void deleteSampleSizeEntryFromDBMS(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTable = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
		moveTable(tempTable, metaSizeTableName);
	}
	
	@Override
	public void cacheTable(TableUniqueName tableName) {
		if (vc.getConf().cacheSparkSamples()) {
			sqlContext.cacheTable(tableName.toString());
		}
	}

	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
	}

	@Override
	public String modOfHash(String col, int mod) {
		return String.format("crc32(cast(%s as string)) %% %d", col, mod);
	}
	
	@Override
	public boolean isSpark() {
		return true;
	}

	@Override
	public void close() throws VerdictException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public String modOfRand(int mod) {
		return String.format("pmod(abs(rand(unix_timestamp())), %d)", mod);
	}
	
}
