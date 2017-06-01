package edu.umich.verdict.query;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;

public class VerdictCreateTableAsSelectQueryTest {

	public VerdictCreateTableAsSelectQueryTest() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost");
		conf.setPort("3306");
		conf.setDbmsSchema("tpch1G");
		conf.setUser("verdict");
		conf.setPassword("verdict");
		VerdictContext vc = new VerdictContext(conf);
		String sql = "CREATE TABLE test AS SELECT COUNT(*) FROM LINEITEM";
		
		vc.executeQuery(sql);
	}

}
