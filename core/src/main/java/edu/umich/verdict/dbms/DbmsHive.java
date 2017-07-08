package edu.umich.verdict.dbms;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Joiner;

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
import edu.umich.verdict.util.VerdictLogger;

public class DbmsHive extends DbmsImpala {

	public DbmsHive(VerdictContext vc, String dbName, String host, String port, String schema, String user,
			String password, String jdbcClassName) throws VerdictException {
		super(vc, dbName, host, port, schema, user, password, jdbcClassName);
	}
	
	@Override
	public String getQuoteString() {
		return "`";
	}
	
	@Override
	public ResultSet describeTable(TableUniqueName tableUniqueName)  throws VerdictException {
		return executeQuery(String.format("describe %s", tableUniqueName));
	}
	
	@Override
	public ResultSet getDatabaseNames() throws VerdictException {
		return executeQuery("show databases");
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
		VerdictLogger.debug(this, "Done.");
	}
	
	@Override
	public String modOfHash(String col, int mod) {
//		return String.format("pmod(conv(substr(md5(cast(%s AS string)),17,16),16,10), %d)", col, mod);
//		return String.format("pmod(conv(substr(md5(%s),17,16),16,10), %d)", col, mod);
		return String.format("pmod(crc32(%s),%d)", col, mod);
	}

	@Override
	public Pair<Long, Long> createUniformRandomSampleTableOf(SampleParam param) throws VerdictException {
		dropTable(param.sampleTableName());
		
		String samplingProbColInQuote = quote(vc.getDbms().samplingProbabilityColumnName());
		List<String> colNames = vc.getMeta().getColumnNames(param.originalTable);
		
		ExactRelation sampled = SingleRelation.from(vc, param.originalTable)
                .select("*, count(*) OVER () AS " + quote("__total_size"))
		        .where(CompCond.from(Expr.from("rand(unix_timestamp())"), "<", ConstantExpr.from(param.samplingRatio)));
		sampled = sampled.select(
				Joiner.on(", ").join(colNames) +
				", count(*) over () / " + quote("__total_size") + " AS " + samplingProbColInQuote + ", " +  // attach sampling prob
				randomPartitionColumn());										 // attach partition number
		
		String sql = String.format("create table %s AS ", param.sampleTableName()) + sampled.toSql();
		VerdictLogger.debug(this, "The query used for creating a uniform random sample:");
		VerdictLogger.debugPretty(this, Relation.prettyfySql(sql), "  ");
		
		executeUpdate(sql);
		
		return Pair.of(getTableSize(param.sampleTableName()), getTableSize(param.originalTable));
	}
	
	protected String quote(String expr) {
		return String.format("`%s`", expr);
	}
	
}
