package edu.umich.verdict.relation;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SubsamplingNestedQueryTest {

	public static void main(String[] args) throws VerdictException {
		
		VerdictConf conf = new VerdictConf();
		conf.setHost("salat1.eecs.umich.edu");
		conf.setDbms("impala");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");
		VerdictContext vc = VerdictContext.from(conf);
		
		String sql;
		ExactRelation r;
		String converted;
		ResultSet rs;
		
		sql = "select avg(days_since_prior) from orders where days_since_prior > (select avg(days_since_prior) from orders)";
		r = ExactRelation.from(vc, sql);
		rs = vc.executeQuery(sql);
		ResultSetConversion.printResultSet(rs);
	}

}
