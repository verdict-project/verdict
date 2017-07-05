package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;

import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.datatypes.SampleParam;
import edu.umich.verdict.datatypes.SampleSizeInfo;
import edu.umich.verdict.datatypes.TableUniqueName;
import edu.umich.verdict.datatypes.VerdictResultSet;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.relation.ExactRelation;
import edu.umich.verdict.relation.Relation;
import edu.umich.verdict.relation.SingleRelation;
import edu.umich.verdict.relation.condition.CompCond;
import edu.umich.verdict.relation.ApproxRelation;
import edu.umich.verdict.relation.ApproxSingleRelation;
import edu.umich.verdict.relation.expr.BinaryOpExpr;
import edu.umich.verdict.relation.expr.ColNameExpr;
import edu.umich.verdict.relation.expr.ConstantExpr;
import edu.umich.verdict.relation.expr.Expr;
import edu.umich.verdict.relation.expr.SubqueryExpr;
import edu.umich.verdict.util.VerdictLogger;

public class DbmsImpala extends Dbms {

	public DbmsImpala(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
		// TODO Auto-generated constructor stub
	}

	// @return temp table name
	@Deprecated
	protected TableUniqueName createTempTableWithRand(TableUniqueName originalTableName) throws VerdictException {
		TableUniqueName tempTableName = Relation.getTempTableName(vc);
		VerdictLogger.debug(this, "Creates a temp table with random numbers: " + tempTableName);
		ExactRelation withRand = SingleRelation.from(vc, originalTableName)
				                 .select("*, rand(unix_timestamp()) as verdict_rand, count(*) over () AS __total_size");
		String sql = String.format("CREATE TABLE %s AS ", tempTableName)
				     + withRand.toSql();
		VerdictLogger.debug(this, "The query used for creating a temp table used for a uniform random sample:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
		VerdictLogger.debug(this, "Done.");
		return tempTableName;
	}
	
	@Deprecated
	protected void createUniformSampleTableFromTempTable(TableUniqueName tempTableName, SampleParam param)
			throws VerdictException {
		VerdictLogger.debug(this, "Creates a sample table of " + tempTableName);
		List<String> colNames = vc.getMeta().getColumnNames(param.originalTable);
		String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();
		String sql = String.format("CREATE TABLE %s AS ", param.sampleTableName()) + 
					 SingleRelation.from(vc, tempTableName)
				 	 .where("verdict_rand < " + param.samplingRatio)
					 .select(Joiner.on(", ").join(colNames) + ", count(*) over () / __total_size AS " + samplingProbColName).toSql();
		VerdictLogger.debug(this, "The query used for creating a uniform random sample form a temp table:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
		VerdictLogger.debug(this, "Done.");
	}

	@Override
	public Pair<Long, Long> createUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
		dropTable(param.sampleTableName());
//		TableUniqueName tempTableName = createTempTableWithRand(param.originalTable);
//		createUniformSampleTableFromTempTable(tempTableName, param);
//		dropTable(tempTableName);
		
		String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();
		List<String> colNames = vc.getMeta().getColumnNames(param.originalTable);
		
		// This query exploits the fact that if subquery is combined with a regular column in a comparison condition,
		// Impala properly generates a random number for every row and makes a comparison.
		Expr threshold = ConstantExpr.from(String.format("one * (select %f from %s limit 1)", param.samplingRatio, param.originalTable));
		ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
				                .select("*, 1 AS one, count(*) OVER () AS __total_size")
						        .where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", threshold));
		sampled = sampled.select(
					Joiner.on(", ").join(colNames) +
					", count(*) over () / __total_size AS " + samplingProbColName);		// attach sampling prob
		String sql = String.format("create table %s AS ", param.sampleTableName()) + sampled.toSql();
		VerdictLogger.debug(this, "The query used for creating a stratified sample:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		
		executeUpdate(sql);
		
		return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
	}
	
	protected TableUniqueName createTempTableExlucdingNameEntry(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = Relation.getTempTableName(vc);
		TableUniqueName originalTableName = param.originalTable;
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s "
				+ "WHERE originalschemaname <> \"%s\" OR originaltablename <> \"%s\" OR sampletype <> \"%s\""
				+ "OR samplingratio <> %s OR columnnames <> \"%s\"",
				tempTableName, metaNameTableName, originalTableName.schemaName, originalTableName.tableName,
				param.sampleType, samplingRatioToString(param.samplingRatio), columnNameListToString(param.columnNames)));
		return tempTableName;
	}
	
	@Override
	public void updateSampleNameEntryIntoDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingNameEntry(param, metaNameTableName);
		insertSampleNameEntryIntoDBMS(param, tempTableName);
		VerdictLogger.debug(this, "Created a temp table with the new sample name info: " + tempTableName);
		
		// copy temp table to the original meta name table after inserting a new entry.
		VerdictLogger.debug(this, String.format("Moves the temp table (%s) to the meta name table (%s).", tempTableName, metaNameTableName));
		dropTable(metaNameTableName);
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s", metaNameTableName, tempTableName));
		dropTable(tempTableName);
	}
	
	protected TableUniqueName createTempTableExlucdingSizeEntry(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = Relation.getTempTableName(vc);
		TableUniqueName sampleTableName = param.sampleTableName();
		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s WHERE schemaname <> \"%s\" OR tablename <> \"%s\" ",
				tempTableName, metaSizeTableName, sampleTableName.schemaName, sampleTableName.tableName));
		return tempTableName;
	}
	
	@Override
	public void updateSampleSizeEntryIntoDBMS(SampleParam param, long sampleSize, long originalTableSize, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTableName = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
		insertSampleSizeEntryIntoDBMS(param, sampleSize, originalTableSize, tempTableName);
		VerdictLogger.debug(this, "Created a temp table with the new sample size info: " + tempTableName);
		
		// copy temp table to the original meta size table after inserting a new entry.
		dropTable(metaSizeTableName);
		moveTable(tempTableName, metaSizeTableName);
//		executeUpdate(String.format("CREATE TABLE %s AS SELECT * FROM %s", metaSizeTableName, tempTableName));
//		VerdictLogger.debug(this, String.format("Moved the temp table (%s) to the meta size table (%s).", tempTableName, metaSizeTableName));
//		dropTable(tempTableName);
	}
	
	@Override
	public ExactRelation augmentWithRandomPartitionNum(ExactRelation r) {
		return r.select("*, " + randomPartitionColumn());
	}

	/**
	 * Creates a universe sample table without dropping an old table.
	 * @param originalTableName
	 * @param sampleRatio
	 * @throws VerdictException
	 */
	@Override
	protected void justCreateUniverseSampleTableOf(SampleParam param) throws VerdictException {
		TableUniqueName sampleTableName = param.sampleTableName();
		List<String> colNames = vc.getMeta().getColumnNames(param.originalTable);
		String samplingProbCol = vc.getDbms().samplingProbabilityColumnName();
				
		ExactRelation withSize = SingleRelation.from(vc, param.originalTable)
		 					     .select("*, count(*) over () AS __total_size");
		ExactRelation sampled = withSize.where(
									String.format(
										"abs(fnv_hash(%s)) %% 10000 <= %.4f",
										param.columnNames.get(0),
										param.samplingRatio*10000))
					 			.select(Joiner.on(", ").join(colNames) + ", count(*) over () / __total_size AS " + samplingProbCol);
		
		String sql = String.format("CREATE TABLE %s AS ", sampleTableName)
				     + sampled.toSql();
		
		VerdictLogger.debug(this, String.format("Creates a table: %s using the following statement:", sampleTableName));
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		this.executeUpdate(sql);
		VerdictLogger.debug(this, "Done.");
	}
	
	private String randomPartitionColumn() {
		int pcount = partitionCount();
		return String.format("round(rand(unix_timestamp())*%d) %% %d AS %s", pcount, pcount, partitionColumnName());
	}
	
	private String columnNamesInString(TableUniqueName tableName) {
		return columnNamesInString(tableName, tableName.tableName);
	}
	
	private String columnNamesInString(TableUniqueName tableName, String subTableName) {
		List<String> colNames = vc.getMeta().getColumnNames(tableName);
		List<String> colNamesWithTable = new ArrayList<String>();
		for (String c : colNames) {
			colNamesWithTable.add(String.format("%s.%s", subTableName, c));
		}
		return Joiner.on(", ").join(colNamesWithTable);
	}
	
	/**
	 * Creates a temp table that includes
	 * 1. all the columns in the original table.
	 * 2. the size of the group on which this stratified sample is being created.
	 * 3. a random number between 0 and 1.
	 * @param param
	 * @return A pair of the table with random numbers and the table that stores the per-group size.
	 * @throws VerdictException
	 */
	@Deprecated
	protected Pair<TableUniqueName, TableUniqueName> createTempTableWithGroupCountsAndRand(SampleParam param) throws VerdictException {
		TableUniqueName rnTempTable = Relation.getTempTableName(vc);
		TableUniqueName grpTempTable = Relation.getTempTableName(vc);
		
		TableUniqueName originalTableName = param.originalTable;
		String groupName = Joiner.on(", ").join(param.columnNames);
		
		String sql1 = String.format("CREATE TABLE %s AS SELECT %s, COUNT(*) AS total_grp_size FROM %s GROUP BY %s",
									grpTempTable, groupName, originalTableName, groupName);
		VerdictLogger.debug(this, "The query used for the group-size temp table: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql1), "  ");
		executeUpdate(sql1);
		
//		String sql2 = String.format("CREATE TABLE %s AS SELECT %s, verdict_grp_size, rand(unix_timestamp()) as verdict_rand ",
//									rnTempTable, columnNamesInString(originalTableName))
//				+ String.format("FROM %s, %s", originalTableName, grpTempTable);
		
		List<String> joinCond = new ArrayList<String>();
		for (String g : param.columnNames) {
			joinCond.add(String.format("%s = %s", g, g));
		}
		Relation tableWithRand = SingleRelation.from(vc, originalTableName)
								 .join(SingleRelation.from(vc, grpTempTable),
								 	   Joiner.on(" AND ").join(joinCond))
								 .select(String.format("%s, total_grp_size, rand(unix_timestamp()) as verdict_rand",
								 		 columnNamesInString(originalTableName)));
		String sql2 = String.format("CREATE TABLE %s AS %s", rnTempTable, tableWithRand.toSql());
		
//		sql2 = sql2 + " WHERE " + Joiner.on(" AND ").join(joinCond);
		
		VerdictLogger.debug(this, "The query used for the temp table with group counts and random numbers.");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql2), "  ");
		
		executeUpdate(sql2);
		return Pair.of(rnTempTable, grpTempTable);
	}
	
	/**
	 * 
	 */
	@Override
	protected void justCreateStratifiedSampleTableof(SampleParam param) throws VerdictException {
		SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(param.originalTable, "uniform", null, new ArrayList<String>()));
		long originalTableSize = info.originalTableSize;
		double samplingProbability = param.samplingRatio;
		String groupName = Joiner.on(", ").join(param.columnNames);
		String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();
		TableUniqueName sampleTable = param.sampleTableName();
		String allColumns = Joiner.on(", ").join(vc.getMeta().getColumnNames(param.originalTable));
		
		ExactRelation groupNumRel = SingleRelation.from(vc, param.originalTable)
									.countDistinct(groupName);
		Expr threshold = BinaryOpExpr.from(
				ConstantExpr.from(originalTableSize),
				BinaryOpExpr.from(
						BinaryOpExpr.from(
								ConstantExpr.from(samplingProbability),
								ConstantExpr.from("grp_size"), "/"),
						SubqueryExpr.from(groupNumRel),
						"/"),
				"*");
		ExactRelation sampleWithGrpSize
		  = SingleRelation.from(vc, param.originalTable)
			.select(Arrays.asList("*", String.format("count(*) over (partition by %s) AS grp_size", groupName)))
			.where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", threshold));
		ExactRelation sampleWithSamplingProb
		  = sampleWithGrpSize.select(
				  allColumns + ", "
		          + String.format("count(*) over (partition by %s) / grp_size AS %s", groupName, samplingProbColName));
		
		System.out.println(sampleWithSamplingProb.toSql());
		String sql = String.format("CREATE TABLE %s AS ", sampleTable)
					 + sampleWithSamplingProb.toSql();
		VerdictLogger.debug(this, "The query used for creating a stratified sample:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		executeUpdate(sql);
		
//		Pair<TableUniqueName, TableUniqueName> tempTables = createTempTableWithGroupCountsAndRand(param);
//		TableUniqueName rnTempTable = tempTables.getLeft();
//		TableUniqueName grpTempTable = tempTables.getRight();
//		createStratifiedSampleFromTempTable(rnTempTable, grpTempTable, param);
//		dropTable(rnTempTable);
//		dropTable(grpTempTable);
	}
	
	/**
	 * Creates a stratified sample from a temp table, which is created by
	 *  {@link DbmsImpala#createTempTableWithGroupCountsAndRand createTempTableWithGroupCountsAndRand}.
	 * The created stratified sample includes a sampling probability for every tuple (in column name "verdict_sampling_prob")
	 * so that it can be used for computing the final answer.
	 * 
	 * The sampling probability for each tuple is determined as:
	 *   min( 1.0, (original table size) * (sampling ratio param) / (number of groups) / (size of the group) )
	 * 
	 * @param tempTableName
	 * @param param
	 * @throws VerdictException
	 */
	@Deprecated
	protected void createStratifiedSampleFromTempTable(TableUniqueName rnTempTable, TableUniqueName grpTempTable, SampleParam param)
			throws VerdictException
	{
		TableUniqueName originalTableName = param.originalTable;
		TableUniqueName sampleTempTable = Relation.getTempTableName(vc);
		String samplingProbColName = vc.getDbms().samplingProbabilityColumnName();
		
		VerdictLogger.debug(this, "Creating a sample table using " + rnTempTable + " and " + grpTempTable);
		SampleSizeInfo info = vc.getMeta().getSampleSizeOf(new SampleParam(param.originalTable, "uniform", null, new ArrayList<String>()));
		long originalTableSize = info.originalTableSize;
		
		long groupCount = SingleRelation.from(vc, grpTempTable).countValue();
		String tmpCol1 = Relation.genColumnAlias();
		
		// create a sample table without the sampling probability
		String sql1 = String.format("CREATE TABLE %s AS ", sampleTempTable) 
		   		    + String.format("SELECT %s, %s FROM ", columnNamesInString(originalTableName, "t1"), randomPartitionColumn())
				    + String.format("(SELECT *, %d*%f/%d/total_grp_size AS %s FROM %s) t1 ",
				    				originalTableSize, param.samplingRatio, groupCount, tmpCol1, rnTempTable)
				                  + "WHERE verdict_rand <= " + tmpCol1;
		VerdictLogger.debug(this, "The query used for sample creation without sampling probabilities: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql1), "  ");
		executeUpdate(sql1);
		
		// attach sampling probability
		List<String> joinCond = new ArrayList<String>();
		for (String g : param.columnNames) {
			joinCond.add(String.format("%s = %s", g, g));
		}
		
		ExactRelation grpRatioBase = SingleRelation.from(vc, sampleTempTable)
				 					 .groupby(param.columnNames)
				 					 .agg("count(*) AS sample_grp_size")
				 					 .join(
				 					   SingleRelation.from(vc, grpTempTable),
				 					   Joiner.on(" AND ").join(joinCond));
		List<String> groupNamesWithTabName = new ArrayList<String>();
		for (String col : param.columnNames) {
			groupNamesWithTabName.add(grpTempTable + "." + col);
		}
		ExactRelation grpRatioRel = grpRatioBase
								    .select(
								      Joiner.on(", ").join(groupNamesWithTabName)
								      + String.format(", sample_grp_size / total_grp_size AS %s", samplingProbColName)); 
		ExactRelation stSampleRel = SingleRelation.from(vc, sampleTempTable)
				                    .join(
				                      grpRatioRel,
				                      Joiner.on(" AND ").join(joinCond))
				    			    .select(columnNamesInString(originalTableName, sampleTempTable.tableName)
				    			    		+ String.format(", %s", samplingProbColName)
				    			    		+ String.format(", %s", partitionColumnName()));
		String sql2 = String.format("CREATE TABLE %s AS ", param.sampleTableName()) + stSampleRel.toSql();
		VerdictLogger.debug(this, "The query used for sample creation with sampling probabilities: ");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql2), "  ");
		executeUpdate(sql2);
		
		dropTable(sampleTempTable);
	}
	
	@Override
	public void deleteSampleNameEntryFromDBMS(SampleParam param, TableUniqueName metaNameTableName) throws VerdictException {
		TableUniqueName tempTable = createTempTableExlucdingNameEntry(param, metaNameTableName);
		dropTable(metaNameTableName);
		moveTable(tempTable, metaNameTableName);
	}
	
	@Override
	public void deleteSampleSizeEntryFromDBMS(SampleParam param, TableUniqueName metaSizeTableName) throws VerdictException {
		TableUniqueName tempTable = createTempTableExlucdingSizeEntry(param, metaSizeTableName);
		dropTable(metaSizeTableName);
		moveTable(tempTable, metaSizeTableName);
	}
	
	@Override
	public ResultSet getDatabaseNames() throws VerdictException {
		try {
			ResultSet rs = conn.getMetaData().getSchemas(null, "%");
			Map<Integer, Integer> colMap = new HashMap<Integer, Integer>();
			colMap.put(1, 1);
			return new VerdictResultSet(rs, null, colMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	@Override
	public List<Pair<String, String>> getAllTableAndColumns(String schemaName) throws VerdictException {
		List<Pair<String, String>> tabCols = new ArrayList<Pair<String, String>>();
		
		try {
			List<String> tables = new ArrayList<String>();
			
			ResultSet tableRS = getTableNames(schemaName);
			while (tableRS.next()) {
				String table = tableRS.getString(1);
				tables.add(table);
			} 
			tableRS.close();
			
			for (String table : tables) {
				ResultSet columnRS = describeTable(TableUniqueName.uname(schemaName, table));
				while (columnRS.next()) {
					String column = columnRS.getString(1);
					tabCols.add(Pair.of(table, column));
				}
				columnRS.close();
			}
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		
		return tabCols;
	}
	
	@Override
	public ResultSet getTableNames(String schemaName) throws VerdictException {
		String[] types = {"TABLE"};
		ResultSet rs;
		try {
			rs = conn.getMetaData().getTables(null, schemaName, "%", types);
			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
			columnMap.put(1, 3);	// table name
			return new VerdictResultSet(rs, null, columnMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
	
	@Override
	public ResultSet describeTable(TableUniqueName tableUniqueName)  throws VerdictException {
		try {
			ResultSet rs = conn.getMetaData().getColumns(
					null, tableUniqueName.schemaName, tableUniqueName.tableName, "%");
			Map<Integer, Integer> columnMap = new HashMap<Integer, Integer>();
			columnMap.put(1, 4);	// column name
			columnMap.put(2, 6); 	// data type name
			columnMap.put(3, 12); 	// remarks
			return new VerdictResultSet(rs, null, columnMap);
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
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
	public boolean doesMetaTablesExist(String schemaName) throws VerdictException {
		String[] types = {"TABLE"};
		try {
			ResultSet rs = vc.getDbms().getDbmsConnection().getMetaData().getTables(
					null, schemaName, vc.getMeta().getMetaNameTableName(currentSchema.get()).tableName, types);
			if (!rs.next()) return false;
			else return true;
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
	}
}
