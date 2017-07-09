package edu.umich.verdict;

import java.sql.ResultSet;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import edu.umich.verdict.dbms.DbmsSpark;
import edu.umich.verdict.exceptions.VerdictException;

/**
 * Spark 1.6 support
 * @author Yongjoo Park
 *
 */
public class VerdictSparkContext extends VerdictContext {

	public VerdictSparkContext(SQLContext sqlContext) throws VerdictException {
		this(sqlContext, new VerdictConf());
		conf.setDbms("spark");
		setDbms(new DbmsSpark(this, sqlContext));
	}
	
	public VerdictSparkContext(SQLContext sqlContext, VerdictConf conf) throws VerdictException {
		super(conf);
	}
	
	public DataFrame executeSparkQuery(String sql) throws VerdictException {
		DataFrame df = ((DbmsSpark) getDbms()).executeSparkQuery(sql);
		return df;
	}

	@Override
	public void execute(String sql) throws VerdictException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ResultSet getResultSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataFrame getDataFrame() {
		// TODO Auto-generated method stub
		return null;
	}
}
