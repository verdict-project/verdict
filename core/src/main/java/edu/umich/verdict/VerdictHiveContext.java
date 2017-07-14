package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Issues queries through Spark's HiveContext. Supports Spark 1.6.
 * @author Yongjoo Park
 *
 */
public class VerdictHiveContext extends VerdictContext {
	
	private DataFrame df;

	public VerdictHiveContext(SparkContext sc) throws VerdictException {
		this(sc, new VerdictConf());
	}
	
	public VerdictHiveContext(SparkContext sc, VerdictConf conf) throws VerdictException {
		super(conf);
		conf.setDbms("spark");
		HiveContext sqlContext = new HiveContext(sc);
		setDbms(new DbmsSpark(this, sqlContext));
		setMeta(new VerdictMeta(this));
	}

	@Override
	public void execute(String sql) throws VerdictException {
		VerdictLogger.debug(this, "An input query:");
		VerdictLogger.debugPretty(this, sql, "  ");
		Query vq = Query.getInstance(this, sql);
		df = vq.computeDataFrame();
	}

	@Override
	public ResultSet getResultSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataFrame getDataFrame() {
		return df;
	}
}
