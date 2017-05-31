package edu.umich.verdict.mysql;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class InstacartTest {
	
	public static void main(String[] args) throws VerdictException {
		VerdictConf conf = new VerdictConf();
		conf.setDbms("mysql");
		conf.setHost("localhost:3306");
		conf.setDbmsSchema("instacart1g");
		conf.set("user", "root");

		VerdictContext vc = new VerdictContext(conf);
		
//		createSamples(vc);
		
//		ResultSet rs1 = vc.executeQuery("SELECT departments.department_id, department, count(*) as order_count"
//				+ " FROM orders, order_products, products, departments"
//				+ " WHERE orders.order_id = order_products.order_id"
//				+ "   AND order_products.product_id = products.product_id"
//				+ "   AND products.department_id = departments.department_id"
//				+ " GROUP BY department_id, department"
//				+ " ORDER BY order_count DESC");
//		ResultSetConversion.printResultSet(rs1);
		
//		Answer answer1 = vc.executeQuery("select order_hour_of_day, count(*) from orders group by order_hour_of_day order by order_hour_of_day;");
//		answer1.print();
		
//		Answer answer2 = vc.executeQuery("select order_dow, count(*) from orders group by order_dow order by order_dow");
//		answer2.print();
		
//		Answer answer3 = vc.executeQuery("select order_hour_of_day, count(*)"
//				+ " from orders, order_products"
//				+ " where orders.order_id = order_products.order_id"
//				+ " group by order_hour_of_day"
//				+ " order by order_hour_of_day");
//		answer3.print();
		
//		Answer answer4 = vc.executeQuery("select order_dow, count(*)"
//				+ " from orders, order_products"
//				+ " where orders.order_id = order_products.order_id"
//				+ " group by order_dow"
//				+ " order by order_dow");
//		answer4.print();
		
//		Answer answer5 = vc.executeQuery("select order_dow, avg(order_count)"
//				+ " from (select user_id, order_dow, count(*) as order_count"
//				+ "       from orders, order_products"
//				+ "       where orders.order_id = order_products.order_id"
//				+ "       group by user_id) t"
//				+ " group by order_dow"
//				+ " order by order_dow");
//		answer5.print();
	}
	
	public static void createSamples(VerdictContext vc) throws VerdictException {
		vc.executeQuery("create sample from orders");
		vc.executeQuery("create sample from order_products");
	}
}
