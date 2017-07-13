package edu.umich.verdict

import edu.umich.verdict.VerdictSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


class SparkIntegrationSuite {
    
    def main(args: Array[String]) {
        
        val conf = new SparkConf()
        conf.setMaster("spark://salat1.eecs.umich.edu:7077")
        conf.setAppName("Spark Integration Tests")
        val sc = new SparkContext(conf);
        val sqlContext = new HiveContext(sc)
		val vc = new VerdictSparkContext(sqlContext)
		val sql: String = "select count(*) from instacart1g.orders"
		val df: DataFrame = vc.sql(sql)
		df.show(false)
    }
    
}
