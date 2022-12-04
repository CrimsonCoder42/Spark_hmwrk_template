package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{ LazyLogging }
import org.cscie88c.config.{ ConfigUtils }
import org.cscie88c.utils.{ SparkUtils }
import org.apache.spark.sql.{ Dataset }
import pureconfig.generic.auto._

// write config case class below
case class SparkDSConfig(
    name: String,
    masterUrl: String,
    transactionFile: String
  )

// run with: sbt "runMain org.cscie88c.week9.SparkDSApplication"
object SparkDSApplication {

  val SPARK_DS_PATH = "org.cscie88c.spark-ds-application"

  // application main entry point
  def main(args: Array[String]): Unit = {
    implicit val conf: SparkDSConfig = readConfig()
    val spark = SparkUtils.sparkSession(conf.name, conf.masterUrl)
    val transactionDS = loadData(spark)
    val totalsByCategoryDS = transactionTotalsByCategory(spark, transactionDS)
    printTransactionTotalsByCategory(totalsByCategoryDS)
    spark.stop()
  }

  def readConfig(): SparkDSConfig =
    ConfigUtils.loadAppConfig[SparkDSConfig](SPARK_DS_PATH)

  def loadData(
      spark: SparkSession
    )(implicit
      conf: SparkDSConfig
    ): Dataset[CustomerTransaction] = {
    import spark.implicits._
    val cleanContent = MapCollect.mapCollectCase(
      spark.sparkContext.textFile(conf.transactionFile)
    )
    cleanContent.toDS()
  }

  def transactionTotalsByCategory(
      spark: SparkSession,
      transactions: Dataset[CustomerTransaction]
    ): Dataset[(String, Double)] = {
    import spark.implicits._
    val groupDS = transactions.groupByKey(
      _.transactionCategory
    ) //https://www.javatpoint.com/apache-spark-groupbykey-function
    val mappedgroupDS = groupDS.mapValues(
      _.transactionAmount
    ) //https://www.scala-lang.org/api/2.13.1/scala/collection/MapView$$MapValues.html
    mappedgroupDS.reduceGroups(
      _ + _
    ) // https://medium.com/build-and-learn/spark-aggregating-your-data-the-fast-way-e37b53314fad
  }
  //https://sparkbyexamples.com/spark/spark-show-display-dataframe-contents-in-table/
  def printTransactionTotalsByCategory(ds: Dataset[(String, Double)]): Unit =
    ds.show()
}
