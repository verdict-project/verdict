package edu.umich.verdict;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import edu.umich.verdict.exceptions.VerdictException;

@Category(IntegrationTest.class)
public class ImpalaAggregationIT extends AggregationIT {
	
	@BeforeClass
	public static void connect() throws VerdictException, SQLException, FileNotFoundException {
		final String host = readHost();
		final String port = "21050";
		final String schema = "instacart1g";
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		vc = new VerdictContext(conf);
		
		String url = String.format("jdbc:impala://%s:%s/%s", host, port, schema);
		Connection conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
	@AfterClass
	public static void destroy() throws VerdictException {
		vc.destroy();
	}

	@Override
	public void simpleAvgUsingUniformSample() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleAvgUsingUniformSample();
	}
	
	

}
