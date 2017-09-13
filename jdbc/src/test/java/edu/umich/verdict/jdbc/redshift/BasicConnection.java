package edu.umich.verdict.jdbc.redshift;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class BasicConnection {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, VerdictException {

        Class.forName("edu.umich.verdict.jdbc.Driver");

        String url = "jdbc:verdict:redshift://verdict-redshift-demo.crc58e3qof3k.us-east-1.redshift.amazonaws.com:5439/dev;UID=admin;PWD=qKUcr2CUgSP3NjHE";
        Connection conn = DriverManager.getConnection(url);
        Statement statement = conn.createStatement();
        
        statement.executeQuery("use tpch100g_demo");
        
        statement.executeQuery("select extract(month from o_orderdate) as m,\n" + 
                "       extract(day from o_orderdate),\n" + 
                "       count(*)\n" + 
                "from orders_lineitem\n" + 
                "group by extract(month from o_orderdate),\n" + 
                "         extract(day from o_orderdate)\n" + 
                "order by m, extract(day from o_orderdate)\n" + 
                "limit 10;\n");
        
    }

}
