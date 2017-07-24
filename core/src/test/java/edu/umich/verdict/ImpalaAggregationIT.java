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
	public static void connect() throws VerdictException, SQLException, FileNotFoundException, ClassNotFoundException {
		final String host = readHost();
		final String port = "21050";
		final String schema = "instacart1g";
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		conf.set("verdict.loglevel", "debug");
		conf.set("verdict.meta_catalog_suffix", "_verdict");
		vc = VerdictJDBCContext.from(conf);
		
		String url = String.format("jdbc:impala://%s:%s/%s", host, port, schema);
		Class.forName("com.cloudera.impala.jdbc4.Driver");
		Connection conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
	@AfterClass
	public static void destroy() throws VerdictException {
		vc.destroy();
	}

	@Override
	public void simpleCount() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleCount();
	}

	@Override
	public void simpleAvgUsingStratifiedSample() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleAvgUsingStratifiedSample();
	}

	@Override
	public void simpleAvgUsingStratifiedSample2() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleAvgUsingStratifiedSample2();
	}

	@Override
	public void simpleAvgUsingStratifiedSample3() throws VerdictException, SQLException {
		// TODO Auto-generated method stub
		super.simpleAvgUsingStratifiedSample3();
	}

	

}
