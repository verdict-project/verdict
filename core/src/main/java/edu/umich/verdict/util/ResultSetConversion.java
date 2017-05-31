package edu.umich.verdict.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import edu.umich.verdict.exceptions.VerdictException;

public class ResultSetConversion {
	
	public static void printResultSet(ResultSet rs) throws VerdictException {
		try {
			ResultSetMetaData meta = rs.getMetaData();
			int cols = meta.getColumnCount();
			
			for (int j = 1; j <= cols; j++) {
				System.out.print(String.format("[%s, %s]", meta.getColumnTypeName(j), meta.getColumnLabel(j)) + "\t");
			}
			System.out.println();
			
			while (rs.next()) {
				for (int j = 1; j <= cols; j++) {
					System.out.print(rs.getObject(j) + "\t");
				}
				System.out.println();
			}
			
		} catch (SQLException e) {
			throw new VerdictException(e);
		}
		
	}

}
