package edu.umich.verdict.relation;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class SubsamplingSqlConversionTest {
	
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
		
		sql = "select count(*) from orders";
		r = ExactRelation.from(vc, sql);
//		converted = r.approx().toSql();
//		System.out.println(converted);
//		System.out.println(Relation.prettyfySql(converted));
		rs = vc.executeQuery(sql);
		ResultSetConversion.printResultSet(rs);
		
		sql = "select sum(orders.days_since_prior) from orders";
		r = ExactRelation.from(vc, sql);
//		converted = r.approx().toSql();
//		System.out.println(converted);
//		System.out.println(Relation.prettyfySql(converted));
		rs = vc.executeQuery(sql);
		ResultSetConversion.printResultSet(rs);
		
		sql = "select order_dow, count(*) from orders group by order_dow order by order_dow";
		r = ExactRelation.from(vc, sql);
//		converted = r.approx().toSql();
//		System.out.println(converted);
//		System.out.println(Relation.prettyfySql(converted));
		rs = vc.executeQuery(sql);
		ResultSetConversion.printResultSet(rs);
		
		vc.destroy();
	}

}
