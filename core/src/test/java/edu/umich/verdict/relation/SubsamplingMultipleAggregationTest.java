package edu.umich.verdict.relation;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SubsamplingMultipleAggregationTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setHost("salat1.eecs.umich.edu");
		conf.setDbms("impala");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = new VerdictContext(conf);
		
		String sql;
		ExactRelation r;
		String converted;
		ResultSet rs;
		
//		sql = "select count(distinct user_id), count(*) from orders";
//		rs = vc.executeQuery(sql);
//		ResultSetConversion.printResultSet(rs);
		
//		sql = "select count(distinct user_id), count(distinct order_id) from orders";
//		rs = vc.executeQuery(sql);
//		ResultSetConversion.printResultSet(rs);
		
//		sql = "select count(distinct user_id), count(distinct order_id), count(*) from orders";
//		rs = vc.executeQuery(sql);
//		ResultSetConversion.printResultSet(rs);
		
//		sql = "select order_dow, count(distinct user_id), count(*) from orders group by order_dow order by order_dow";
//		rs = vc.executeQuery(sql);
//		ResultSetConversion.printResultSet(rs);
		
//		sql = "select order_dow AS dow, count(distinct user_id), count(distinct order_id), count(*) from orders group by order_dow order by order_dow";
//		rs = vc.executeQuery(sql);
//		ResultSetConversion.printResultSet(rs);
		
		sql = "select dow AS dow1 from ("
			+ "select order_dow AS dow, count(distinct user_id), count(distinct order_id), count(*) "
			+ "from orders "
			+ "group by order_dow "
			+ "order by order_dow) t1";
		rs = vc.executeQuery(sql);
		ResultSetConversion.printResultSet(rs);
		
		vc.destroy();
	}

}
