package edu.umich.verdict;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import edu.umich.verdict.dbms.Dbms;
import edu.umich.verdict.exceptions.VerdictException;

/**
 * Spark 1.6 support
 * @author Yongjoo Park
 *
 */
public class VerdictSparkContext extends VerdictContext {

	private SQLContext sqlContext;

	public VerdictSparkContext(SQLContext sqlContext) throws VerdictException {
		this(sqlContext, new VerdictConf());
		conf.setDbms("spark");
		setDbms(Dbms.from(this, conf));
	}
	
	public VerdictSparkContext(SQLContext sqlContext, VerdictConf conf) throws VerdictException {
		super(conf);
	}
	
	DataFrame executeSparkQuery(String sql) {
		DataFrame df = sqlContext.sql(sql);
		return df;
	}
}
