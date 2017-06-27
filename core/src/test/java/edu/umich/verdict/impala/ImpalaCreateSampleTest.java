package edu.umich.verdict.impala;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaCreateSampleTest {

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setPort("21050");
		conf.setDbmsSchema("instacart1g");
		conf.set("no_user_password", "true");

		VerdictContext vc = new VerdictContext(conf);
		vc.executeQuery("create stratified sample of orders on order_dow");
		vc.destroy();
		System.out.println("Done");
	}

}
