package edu.umich.verdict.impala;

import java.sql.ResultSet;

import edu.umich.verdict.VerdictConf;
import edu.umich.verdict.VerdictJDBCContext;
import edu.umich.verdict.exceptions.VerdictException;
import edu.umich.verdict.util.ResultSetConversion;

public class ImpalaThroughVerdictTest {

	public static void main(String[] args) throws VerdictException {
		
		VerdictConf conf = new VerdictConf();
		conf.setDbms("impala");
		conf.setHost("ec2-34-202-126-188.compute-1.amazonaws.com:21050");
		conf.setDbmsSchema("instacart100g");

		VerdictJDBCContext vc = VerdictJDBCContext.from(conf);
		
//		vc.executeQuery("create 1% sample from orders");
//		vc.executeQuery("create 1% sample from order_products");
		
//		ResultSet rs0 = vc.executeQuery("select count(*) as nums from orders limit 10");
//		ResultSetConversion.printResultSet(rs0);
		
		// Q1
//		ResultSet rs1 = vc.executeQuery("select count(*) as c from orders");
//		ResultSetConversion.printResultSet(rs1);
		
		System.out.println("Decimal Number: " + java.sql.Types.DECIMAL);
		// Q2
//		ResultSet rs2 = vc.executeQuery("select order_hour_of_day, count(*) as c from orders"
//				+ " group by order_hour_of_day"
//				+ " order by order_hour_of_day");
//		ResultSetConversion.printResultSet(rs2);
		
		// Q3
//		ResultSet rs3 = vc.executeQuery("SELECT product_name, count(*) as order_count"
//				+ "				FROM order_products, orders, products"
//				+ "				WHERE orders.order_id = order_products.order_id"
//				+ "				  AND order_products.product_id = products.product_id"
//				+ "				  AND (order_dow = 0 OR order_dow = 1)"
//				+ "				GROUP BY product_name"
//				+ "				ORDER BY order_count DESC"
//				+ "				LIMIT 5");
//		ResultSetConversion.printResultSet(rs3);

		
		// Q4
		ResultSet rs4 = vc.executeQuery("SELECT departments.department_id, department, count(*) as order_count"
				+ " FROM order_products, orders, products, departments"
				+ " WHERE orders.order_id = order_products.order_id"
				+ "   AND order_products.product_id = products.product_id"
				+ "   AND products.department_id = departments.department_id"
				+ " GROUP BY department_id, department"
				+ " ORDER BY order_count DESC");
		ResultSetConversion.printResultSet(rs4);
		
		// Q5
//		ResultSet rs5 = vc.executeQuery("SELECT 5*round(d1/5) as reorder_after_days, COUNT(*)"
//				+ " FROM (SELECT user_id, AVG(days_since_prior) AS d1, COUNT(*) AS c2"
//				+ "      FROM order_products, orders"
//				+ "      WHERE orders.order_id = order_products.order_id"
//				+ "        AND days_since_prior IS NOT NULL"
//				+ "      GROUP BY user_id) t2"
//				+ " WHERE c2 > (SELECT AVG(c1) AS a1"
//				+ "            FROM (SELECT user_id, COUNT(*) AS c1"
//				+ "                  FROM order_products, orders"
//				+ "                  WHERE orders.order_id = order_products.order_id"
//				+ "                  GROUP BY user_id) t1)"
//				+ " group by reorder_after_days"
//				+ " order by reorder_after_days");
//		ResultSetConversion.printResultSet(rs5);
		
		// Q6
//		ResultSet rs6 = vc.executeQuery("SELECT product_name, count(*) as freq_order_count"
//		+ " FROM order_products, orders, products, departments"
//		+ " WHERE orders.order_id = order_products.order_id"
//		+ "	  AND order_products.product_id = products.product_id"
//		+ "	  AND products.department_id = departments.department_id"
//		+ "	  AND user_id IN ("
//		+ "		          SELECT user_id"
//		+ "		          FROM (SELECT user_id, count(*) as order_count"
//		+ "						FROM orders"
//		+ "						GROUP BY user_id) t1"
//		+ "		          WHERE order_count > (SELECT AVG(order_count) as large_order_count"
//		+ "		                               FROM (SELECT user_id, count(*) as order_count"
//		+ "											 FROM orders"
//		+ "											 GROUP BY user_id) t2 ))"
//		+ "		GROUP BY product_name"
//		+ "		ORDER BY freq_order_count DESC"
//		+ "		LIMIT 10");
//		ResultSetConversion.printResultSet(rs6);
		
		
//		Answer anst1 = vc.executeQuery("select 5*round(days_since_prior/5) as d1, count(*) from orders where days_since_prior is not null group by d1 order by d1");
//		anst1.print();
		
		vc.destroy();
	}

}
