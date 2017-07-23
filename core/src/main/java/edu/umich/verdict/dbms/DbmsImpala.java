package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

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
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsImpala extends DbmsJDBC {

	public DbmsImpala(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
		// TODO Auto-generated constructor stub
	}

	@Override
	public ResultSet getDatabaseNamesInResultSet() throws VerdictException {
		return executeJdbcQuery("show databases");
	}
	
	@Override
	public ResultSet getTablesInResultSet(String schema) throws VerdictException {
		return executeJdbcQuery("show tables in " + schema);
	}

	@Override
	public List<String> getTables(String schema) throws VerdictException {
		List<String> tables = new ArrayList<String>();
		try {
			ResultSet rs = executeJdbcQuery("show tables in " + schema);
			while (rs.next()) {
				String table = rs.getString(1);
				tables.add(table);
			}
		} catch (SQLException e) {
			VerdictLogger.error(this, "Failed to access the database: " + schema);
			throw new VerdictException(e);
		}
		return tables;
	}
	
	public Map<String, String> getColumns(TableUniqueName table) throws VerdictException {
		Map<String, String> col2type = new LinkedHashMap<String, String>();
		try {
			ResultSet rs = executeJdbcQuery("describe " + table);
			while (rs.next()) {
				String column = rs.getString(1);
				String type = rs.getString(2);
				col2type.put(column, type);
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		return col2type;
	}

	@Override
	public ResultSet describeTableInResultSet(TableUniqueName tableUniqueName)  throws VerdictException {
		return executeJdbcQuery(String.format("describe %s", tableUniqueName));
	}

	/**
	 * Impala does not support the standard JDBC protocol {@link java.sql.Connection#setCatalog(String) setCatalog}
	 * function for changing the current database. This is a workaround.
	 */
	@Override
	public void changeDatabase(String schemaName) throws VerdictException {
		execute(String.format("use %s", schemaName));
		currentSchema = Optional.fromNullable(schemaName);
		VerdictLogger.info("Database changed to: " + schemaName);
	}

	@Override
	public void createMetaTablesInDMBS(
			TableUniqueName originalTableName,
			TableUniqueName sizeTableName,
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
	}
	
	private TableUniqueName createUniformRandomSampledTable(SampleParam param) throws VerdictException {
		Map<String, String> col2types = vc.getMeta().getColumn2Types(param.originalTable);
		int precision = 3;
		int modValue = (int) Math.pow(10, precision);
		
		Set<String> hashCols = new HashSet<String>();
		for (Map.Entry<String, String> col2type : col2types.entrySet()) {
			String col = col2type.getKey();
			String type = col2type.getValue();
			if (type.toLowerCase().contains("char") || type.toLowerCase().contains("str")) {
				hashCols.add(String.format("fnv_hash((case when %s is null then cast(unix_timestamp() as string) else %s end))", col, col));
			} else {
				hashCols.add(String.format("fnv_hash((case when %s is null then unix_timestamp() else %s end))", col, col));
			}
		}
		String whereClause = "abs(fnv_hash("
		                     + Joiner.on(" + ").join(hashCols) 
		                     + String.format(" + unix_timestamp())) %% %d < %d", modValue, (int) (param.samplingRatio * modValue));
		
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
	
//	private TableUniqueName createTableWithRandNumbers(SampleParam param) throws VerdictException {
//		ExactRelation withRand = SingleRelation.from(vc, param.originalTable)
//								 .select("*, rand(unix_timestamp()) AS __rand");
//		TableUniqueName temp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
//		String sql = String.format("create table %s AS %s", temp, withRand.toSql());
//		VerdictLogger.debug(this, "The query used for creating a temporary table with a column containing random numbers:");
//		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
//		executeUpdate(sql);
//		return temp;
//	}
	
//	private void createUniformRandomSampleFromRandTable(SampleParam param, TableUniqueName temp) throws VerdictException {
//		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
//		Set<String> colNames = vc.getMeta().getColumns(param.originalTable);
//		
//		ExactRelation withRand = SingleRelation.from(vc, temp);
//		long total_size = withRand.countValue();
//		
//		ExactRelation sampled = withRand
//							   .where("__rand < " + param.samplingRatio)
//							   .select(
//							      Joiner.on(", ").join(colNames) +
//							      ", count(*) over () / " + total_size + " AS " + samplingProbCol + ", " + // attach sampling prob
//							      randomPartitionColumn());
//		String sql = String.format("create table %s AS %s", param.sampleTableName(), sampled.toSql());
//		VerdictLogger.debug(this, "The query used for creating a uniform random sample from a temporary table:");
//		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
//		executeUpdate(sql);
//	}
	
	/**
	 * Creates a universe sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	@Override
	protected void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException {
		TableUniqueName temp = createUniverseSampledTable(param);
		createUniverseSampleWithProbFromSample(param, temp);
		dropTable(temp);
		
//		TableUniqueName sampleTableName = param.sampleTableName();
//		Set<String> colNames = vc.getMeta().getColumns(param.originalTable);
//		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
//				
//		ExactRelation withSize = SingleRelation.from(vc, param.originalTable)
//		 					     .select("*, count(*) over () AS __total_size");
//		ExactRelation sampled = withSize.where(
//									modOfHash(param.columnNames.get(0), 1000000) + 
//									String.format(" < %.2f", param.samplingRatio*1000000))
//					 			.select(Joiner.on(", ").join(colNames)
//					 					+ ", count(*) over () / __total_size AS " + samplingProbCol + ", "
//					 					+ randomPartitionColumn());
//		
//		String sql = String.format("CREATE TABLE %s AS ", sampleTableName)
//				     + sampled.toSql();
//		
//		VerdictLogger.debug(this, String.format("Creates a table: %s using the following statement:", sampleTableName));
//		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
//		this.executeUpdate(sql);
//		VerdictLogger.debug(this, "Done.");
	}
	
	private TableUniqueName createUniverseSampledTable(SampleParam param) throws VerdictException {
		TableUniqueName temp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
				                .where(modOfHash(param.columnNames.get(0), 1000000) + 
				                		   String.format(" < %.2f", param.samplingRatio*1000000));
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

	/**
	 * 
	 */
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
		
//		Pair<TableUniqueName, TableUniqueName> temps = createTempTableWithGroupCountsAndRand(param);
//		TableUniqueName withRandTemp = temps.getLeft();
//		TableUniqueName groupSizeTemp = temps.getRight();
//		
//		createStratifiedSampleFromTempTable(param, withRandTemp, groupSizeTemp);
//		dropTable(withRandTemp);
//		dropTable(groupSizeTemp);
		
//		long originalTableSize = info.originalTableSize;
//		double samplingProbability = param.samplingRatio;
//		String groupName = Joiner.on(", ").join(param.columnNames);
//		String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();
//		TableUniqueName sampleTable = param.sampleTableName();
//		String allColumns = Joiner.on(", ").join(vc.getMeta().getColumns(param.originalTable));
//		
//		ExactRelation groupNumRel = SingleRelation.from(vc, param.originalTable)
//									.countDistinct(groupName);
//		Expr threshold = BinaryOpExpr.from(
//				ConstantExpr.from(originalTableSize),
//				BinaryOpExpr.from(
//						BinaryOpExpr.from(
//								ConstantExpr.from(samplingProbability),
//								ConstantExpr.from("grp_size"), "/"),
//						SubqueryExpr.from(groupNumRel),
//						"/"),
//				"*");
//		ExactRelation sampleWithGrpSize
//		  = SingleRelation.from(vc, param.originalTable)
//			.select(Arrays.asList("*", String.format("count(*) over (partition by %s) AS grp_size", groupName)))
//			.where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", threshold));
//		ExactRelation sampleWithSamplingProb
//		  = sampleWithGrpSize.select(
//				  allColumns + ", "
//		          + String.format("count(*) over (partition by %s) / grp_size AS %s", groupName, samplingProbColName) + ", "
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
		int precision = 3;
		int modValue = (int) Math.pow(10, precision);
		
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
		
		// hash-based random number
		Set<String> hashCols = new HashSet<String>();
		for (Map.Entry<String, String> col2type : col2types.entrySet()) {
			String col = col2type.getKey();
			String type = col2type.getValue();
			if (type.toLowerCase().contains("char") || type.toLowerCase().contains("str")) {
				hashCols.add(String.format("fnv_hash((case when s.%s is null then cast(unix_timestamp() as string) else s.%s end))", col, col));
			} else {
				hashCols.add(String.format("fnv_hash((case when s.%s is null then unix_timestamp() else s.%s end))", col, col));
			}
		}
		String whereClause = "abs(fnv_hash("
		                     + Joiner.on(" + ").join(hashCols) 
		                     + String.format(" + unix_timestamp())) %% %d < %d * %f / %d / __group_size * %d",
                                             modValue,
                                             originalTableSize,
                                             param.getSamplingRatio(),
                                             groupCount,
                                             modValue);
		
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
	
	private Pair<TableUniqueName, TableUniqueName> createTempTableWithGroupCountsAndRand(SampleParam param) throws VerdictException {
		TableUniqueName withRandTemp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		TableUniqueName groupSizeTemp = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		Set<String> colNames = vc.getMeta().getColumns(param.originalTable);
		
		// create a temp table for groupby count
		ExactRelation groupSize = SingleRelation.from(vc, param.originalTable)
								 .groupby(param.columnNames)
								 .agg("count(*) AS __group_size");
		String sql1 = String.format("create table %s AS %s", groupSizeTemp, groupSize.toSql());
		VerdictLogger.debug(this, "The query used for the group-size temp table: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql1), "  ");
		executeUpdate(sql1);
		
		// create a temp table with random numbers
		List<String> joinCond = new ArrayList<String>();
		for (String c : param.columnNames) {
			joinCond.add(String.format("t1.%s = t2.%s OR (t1.%s is null and t2.%s is null)", c, c, c, c));
		}
		
		List<String> colNamesWithTab = new ArrayList<String>();
		for (String c : colNames) {
			colNamesWithTab.add(String.format("t1.%s", c));
		}
		
//		ExactRelation original = SingleRelation.from(vc, param.originalTable).withAlias("t1");
		// temporarily using this because our JoinedRelation does not support arbitrary condition joins.
		String select = String.format("SELECT %s, __group_size, rand(unix_timestamp()) AS __rand ",
									  Joiner.on(", ").join(colNamesWithTab)) +
				        String.format("FROM %s t1 INNER JOIN %s t2 ", param.originalTable, groupSizeTemp) +
				        String.format("ON %s", Joiner.on(" AND ").join(joinCond));
		String sql2 = String.format("create table %s AS %s", withRandTemp, select);
		
		VerdictLogger.debug(this, "The query used for creating a temp table with group counts and random numbers.");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql2), "  ");
		executeUpdate(sql2);
		return Pair.of(withRandTemp, groupSizeTemp);
	}
	
	private void createStratifiedSampleFromTempTable(SampleParam param, TableUniqueName withRandTemp, TableUniqueName groupSizeTemp)
			throws VerdictException
	{
		TableUniqueName sampleTempTable = Relation.getTempTableName(vc, param.sampleTableName().getSchemaName());
		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
		Set<String> colNames = vc.getMeta().getColumns(param.originalTable);
		
		VerdictLogger.debug(this, "Creating a sample table using " + withRandTemp + " and " + groupSizeTemp);
		SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(vc, param.originalTable, "uniform", null, new ArrayList<String>()));
		long originalTableSize = info.originalTableSize;
		long groupCount = SingleRelation.from(vc, groupSizeTemp).countValue();
		
		// create a sample table without the sampling probability
		ExactRelation sampled = SingleRelation.from(vc, withRandTemp)
						        .select(Joiner.on(", ").join(colNames) + ", __rand, " +
						        		    String.format("%d * %f / %d / __group_size AS __threshold",
						        		    		originalTableSize, param.getSamplingRatio(), groupCount))
						        .where("__rand < __threshold")
						        .select(Joiner.on(", ").join(colNames));
		String sql1 = String.format("create table %s AS %s", sampleTempTable, sampled.toSql());
		VerdictLogger.debug(this, "The query used for sample creation without sampling probabilities: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql1), "  ");
		executeUpdate(sql1);
		
		// attach sampling probability
		List<String> joinCond = new ArrayList<String>();
		for (String c : param.columnNames) {
			joinCond.add(String.format("t1.%s = t2.%s", c, c));
		}
		
		ExactRelation grpRatioBase = SingleRelation.from(vc, sampleTempTable)
				                     .groupby(param.columnNames)
				                     .agg("count(*) AS __sample_group_size").withAlias("t1")
				                     .join(SingleRelation.from(vc, groupSizeTemp).withAlias("t2"),
				                    		   Joiner.on(" AND ").join(joinCond));
		
		List<String> groupNamesWithTabName = new ArrayList<String>();
		for (String col : param.columnNames) {
			groupNamesWithTabName.add("t2." + col);
		}
		ExactRelation grpRatioRel = grpRatioBase
				                    .select(Joiner.on(", ").join(groupNamesWithTabName) + ", " + 
				                    	        "__sample_group_size / __group_size AS " + samplingProbCol)
				                    .withAlias("t2");
		
		List<String> colNamesWithTabName = new ArrayList<String>();
		for (String col : colNames) {
			colNamesWithTabName.add("t1." + col);
		}
		ExactRelation stSampleRel = SingleRelation.from(vc, sampleTempTable).withAlias("t1")
		                            .join(grpRatioRel, Joiner.on(" AND ").join(joinCond))
		                            .select(Joiner.on(", ").join(colNamesWithTabName) + ", " + samplingProbCol + ", " +
		                                    randomPartitionColumn());
		String sql2 = String.format("create table %s AS %s", param.sampleTableName(), stSampleRel.toSql());
		VerdictLogger.debug(this, "The query used for sample creation with sampling probabilities: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql2), "  ");
		executeUpdate(sql2);
		
		dropTable(sampleTempTable);
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
	public String modOfHash(String col, int mod) {
		return String.format("abs(fnv_hash(cast(%s AS STRING))) %% %d", col, mod);
	}
	
	protected String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
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
	public String modOfRand(int mod) {
		return String.format("abs(rand(unix_timestamp())) %% %d", mod);
	}

}
