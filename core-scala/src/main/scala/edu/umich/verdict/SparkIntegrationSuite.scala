package edu.umich.verdict

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

class SparkIntegrationSuite {
    
    def test(sqlContext: SQLContext) {
		val vc = new edu.umich.verdict.VerdictSparkContext(sqlContext)
		val sql: String = "select count(*) from instacart1g.orders"
		val expected: DataFrame = sqlContext.sql(sql)
		val actual: DataFrame = vc.sql(sql);
    }
    
}
