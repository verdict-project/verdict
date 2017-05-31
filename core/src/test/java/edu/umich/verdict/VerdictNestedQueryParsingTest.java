package edu.umich.verdict;

import edu.umich.verdict.exceptions.VerdictException;

public class VerdictNestedQueryParsingTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost:3306");
		conf.setDbmsSchema("tpch1G");
		conf.set("user", "root");
		
		VerdictContext vc = new VerdictContext(conf);
		
//		String sql1 = "select avg(C_ACCTBAL)"
//				+ "from customer "
//				+ "where C_ACCTBAL > (select avg(C_ACCTBAL) from customer)";
//		Answer ans1 = vc.executeQuery(sql1);
//		ans1.print();
//		
//		String sql2 = "select avg(C_ACCTBAL)"
//				+ "from (select * from customer) t";
//		Answer ans2 = vc.executeQuery(sql2);
//		ans2.print();
		
//		String sql3 = "select avg(l_quantity) from lineitem, orders where l_orderkey = o_orderkey";
//		Answer ans3 = vc.executeQuery(sql3);
//		ans3.print();
	}

}
