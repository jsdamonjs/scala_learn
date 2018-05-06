package cn.damon.tasks

import cn.damon.util._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}
object test {
  val sparksession = SparkSession.builder().appName("test").master("local[1]").getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val util = new my_util(sparksession)
    val schema_1 = StructType(
      List(
        StructField("label", IntegerType, true),
        StructField("sentence", StringType, true)
      )
    )

    val sentenceData = sparksession.read.option("delimiter","|").schema(schema_1).csv("/home/jiangshuai/test.csv")
    val res = util.tokenizer(sentenceData)
    res.show()
    util.test_func()
    util.my_matrix()

  }
}
