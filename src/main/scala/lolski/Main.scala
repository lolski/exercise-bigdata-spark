package lolski

import org.apache.spark.{SparkConf, SparkContext}
import IO.{parseLogEntry, parseCountryCodeMapEntry, sanitizeCountryName}

// application specific config
object AppConf {
  // spark
  val appName         = "exercise-bigdata-spark"
  val masterUrl       = "local"
  val partitionCount  = 8

  // input
  val baseTmp         = s"${IO.getCwd}/files"
  val countryCodes    = s"$baseTmp/countryCodes.txt" // adapted from https://raw.githubusercontent.com/umpirsky/country-list/master/data/en_US/country.csv
  val logs            = s"$baseTmp/logs"
  val out             = s"$baseTmp/out"
}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(AppConf.appName).setMaster(AppConf.masterUrl)
    val ctx  = new SparkContext(conf)

    // cache as map for constant time get operation, then broadcast it
    val map = ctx.broadcast(
      ctx.textFile(s"file:///${AppConf.countryCodes}")
        .map(parseCountryCodeMapEntry)
        .collectAsMap()
    )

    // collect all logs
    val log = ctx.textFile(s"file:///${AppConf.logs}/*")
      .map { raw =>
        val user = parseLogEntry(raw)
        (user -> user.age)
      }

    val sorted = log repartitionAndSortWithinPartitions(
      partitioner = new PartitionByCountryCode(AppConf.partitionCount))

    val withFileName = sorted map { case (LogEntry(code, _), age) => (sanitizeCountryName(map.value(code)), age) }

    withFileName.saveAsHadoopFile(AppConf.out, classOf[String], classOf[Integer], classOf[KeyAsFileNameOutput[String, Integer]])

    ctx.stop()
  }
}