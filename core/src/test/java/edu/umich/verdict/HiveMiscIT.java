package edu.umich.verdict;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.BeforeClass;
import org.junit.Test;

import edu.umich.verdict.exceptions.VerdictException;

public class HiveMiscIT extends MiscIT {

	@BeforeClass
	public static void connect() throws VerdictException, SQLException, FileNotFoundException {
		final String host = readHost();
		final String port = "10000";
		final String schema = "instacart1g";

		VerdictConf conf = new VerdictConf();
		conf.setDbms("hive2");
		conf.setHost(host);
		conf.setPort(port);
		conf.setDbmsSchema(schema);
		conf.set("no_user_password", "true");
		vc = new VerdictContext(conf);

		String url = String.format("jdbc:hive2://%s:%s/%s", host, port, schema);
		conn = DriverManager.getConnection(url);
		stmt = conn.createStatement();
	}

	@Override
	public void getColumnsTest() throws VerdictException, SQLException {
		super.getColumnsTest();
	}

}
