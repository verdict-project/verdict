package edu.umich.verdict.mysql;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class MysqlDropSampleTest {

	public static void main(String[] args) throws VerdictException {
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost:3306");
		conf.setDbmsSchema("tpch1G");
		conf.set("user", "root");
		VerdictContext vc = new VerdictContext(conf);
		
//		Answer answer = vc.executeQuery("drop sample customer");
		vc.destroy();

		System.out.println("Done");
	}

}
