package edu.umich.verdict.impala;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaCreateSampleTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setHost("salat1.eecs.umich.edu");
		conf.setDbms("impala");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");

		VerdictJDBCContext vc = VerdictJDBCContext.from(conf);
		vc.executeJdbcQuery("create uniform sample of orders");
		vc.destroy();
		System.out.println("Done");
	}

}
