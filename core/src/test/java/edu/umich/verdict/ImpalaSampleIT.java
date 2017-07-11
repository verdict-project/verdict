package edu.umich.verdict;

import java.io.FileNotFoundException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import edu.umich.verdict.exceptions.VerdictException;

public class ImpalaSampleIT extends SampleIT {
	
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
		vc = VerdictJDBCContext.from(conf);
		
		String url = String.format("jdbc:impala://%s:%s/%s", host, port, schema);
		Class.forName("com.cloudera.impala.jdbc4.Driver");
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}
	
	@AfterClass
	public static void destroy() throws VerdictException {
		vc.destroy();
	}

	@Override
	public void showSampleTest() throws VerdictException {
		// TODO Auto-generated method stub
		super.showSampleTest();
	}

	@Override
	public void createUniformSampleTest() throws VerdictException {
		super.createUniformSampleTest();
	}

	@Override
	public void createRecommendedSampleTest() throws VerdictException {
		super.createRecommendedSampleTest();
	}

	@Override
	public void dropRecommendedSampleTest() throws VerdictException {
		super.dropRecommendedSampleTest();
	}

	@Override
	public void createUniverseSampleTest() throws VerdictException {
		super.createUniverseSampleTest();
	}

	@Override
	public void createStratifiedSampleTest() throws VerdictException {
		super.createStratifiedSampleTest();
	}
	
}
