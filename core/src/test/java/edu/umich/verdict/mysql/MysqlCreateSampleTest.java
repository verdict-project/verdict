package edu.umich.verdict.mysql;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class MysqlCreateSampleTest {

	public static void main(String[] args) throws VerdictException {
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost");
		conf.setPort("3306");
		conf.setDbmsSchema("instacart1g");
		conf.setUser("verdict");
		conf.setPassword("verdict");
		VerdictContext vc = new VerdictContext(conf);
		
		vc.executeQuery("create sample of order_products");
		
		vc.destroy();
		
		System.out.println("Done");
	}

}
