package edu.umich.verdict.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import edu.umich.verdict.util.ResultSetConversion;

public class RedshiftJDBCTest {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("com.amazon.redshift.jdbc41.Driver");

		String url = "jdbc:redshift://salat2-verdict.ctkb4oe4rzfm.us-east-1.redshift.amazonaws.com:5439/dev?PWD=BTzyc1xG&UID=junhao";
		Connection conn = DriverManager.getConnection(url);
		Statement stmt = conn.createStatement();
		
		// first query
		boolean isThereResult = stmt.execute("select nspname from pg_namespace");
		System.out.println(isThereResult);
		ResultSet rs = stmt.getResultSet();
		ResultSetConversion.printResultSet(rs);
		rs.close();
		
		// second query
		stmt.executeUpdate("create schema if not exists public_verdict");
		
		// third query
		isThereResult = stmt.execute("select nspname from pg_namespace");
		System.out.println(isThereResult);
		rs = stmt.getResultSet();
		ResultSetConversion.printResultSet(rs);
	}

}
