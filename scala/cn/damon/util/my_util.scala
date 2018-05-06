package cn.damon.util

import cn.damon.tasks.test.sparksession
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer,StopWordsRemover}
import java.util.Date
import java.text.SimpleDateFormat
import Array._
class my_util(sparkSession: SparkSession) {
    //tokenrizer  分词
    private[damon] def tokenizer(dataFrame:DataFrame): DataFrame ={
      val tokenizer = new RegexTokenizer().setInputCol("sentence").setOutputCol("words").setPattern("~").setGaps(true)
      val tokenized = tokenizer.transform(dataFrame)
      val remover = new StopWordsRemover()
        .setInputCol("words")
        .setOutputCol("filtered")
      val res = remover.transform(tokenized)
      return res
//      val getFirst = (arr: Seq[String]) => {arr(1);}
//      sparkSession.udf.register("get_first", getFirst)
//      res.selectExpr("get_first(words) as first_word").show()
    }
    //scala function test  嵌套函数 ，偏应用函数
    private[damon] def test_func(): Unit ={
      def log(date:String,message:String)={
        println(date + "........" + message)
      }
      val my_format = new SimpleDateFormat("yyyy-mm-dd")
      var date = my_format.format(new Date())
      val logwithbound = log(date,_:String)
      logwithbound("first")
      logwithbound("second")
      logwithbound("third")
    }
    private[damon] def my_matrix()={
      val matrix = ofDim[Int](3,3)
      for (i <- 0 to 2){
        for (j <- 0 to 2){
          matrix(i)(j) = 1
        }
      }
      for (i <- 0 to 2){
        for(j <- 0 to 2){
          println(matrix(i)(j))
        }

      }

    }



}
