package org.cscie88c.week9

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.{ LazyLogging }
import org.cscie88c.config.{ ConfigUtils }
import org.cscie88c.utils.{ SparkUtils }
import org.apache.spark.rdd.{ RDD }
import pureconfig.generic.auto._

// New case class to address overlapping calculations.
case class MapCollect()

object MapCollect {

  // https://alvinalexander.com/scala/best-practice-option-some-none-pattern-scala-idioms/
  def mapCollectCase(lines: RDD[String]): RDD[CustomerTransaction] = {
    val incomingLines = lines.map(x => CustomerTransaction(x))
    incomingLines.collect {
      case Some(n) => n
    }
  }

}
