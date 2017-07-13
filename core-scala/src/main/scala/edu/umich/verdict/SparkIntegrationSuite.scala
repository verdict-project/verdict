package edu.umich.verdict

import edu.umich.verdict.VerdictSparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalactic.source.Position.apply


class SparkIntegrationSuite extends FunSuite with SharedSparkContext {

	test("simple count") ({
		val sqlContext = new HiveContext(sc)
		val vc = new VerdictSparkContext(sqlContext)
		val sql: String = "select count(*) from instacart1g.orders"
		val df: DataFrame = vc.sql(sql)
		df.show(false)
	})

}