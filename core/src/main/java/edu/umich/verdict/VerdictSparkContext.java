package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.query.Query;
import edu.umich.verdict.util.VerdictLogger;

/**
 * Spark 1.6 support
 * @author Yongjoo Park
 *
 */
public class VerdictSparkContext extends VerdictContext {
	
	private DataFrame df;

	public VerdictSparkContext(SQLContext sqlContext) throws VerdictException {
		this(sqlContext, new VerdictConf());
		conf.setDbms("spark");
		setDbms(new DbmsSpark(this, sqlContext));
	}
	
	public VerdictSparkContext(SQLContext sqlContext, VerdictConf conf) throws VerdictException {
		super(conf);
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
