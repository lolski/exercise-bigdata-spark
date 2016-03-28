package lolski

import org.apache.spark.{SparkConf, SparkContext}
import IO.{parseLogEntry, parseCountryCodeMapEntry, sanitizeCountryName}

/**
  * Created by lolski on 3/27/16.
  * Problem set 1, part 2
  * Main assumptions:
  *   - using basic spark configuration for local run in a single machine
  *   - country code mapping is assumed to be static, small and needs to be accessed very frequently
  *   - log files are large and may not fit in memory
  * Design decisions:
  *   - country code will be cached for efficiency
  *     - cache in master or broadcast to all nodes? depends on if we want to consolidate output in master or if we want to spread them
  *   - secondary sorting is needed. how to best implement it?
  *     - sortBy works, but doesn't specify anything about partitioning
  *     - groupBy can be used to group by country code, but might be inefficient due to shuffling
  *     - partitioning efficiently by country code and sort within that partition
  *       - hash based or range based? prefer hash based
  * Implementation:
  *   - make a program that supports secondary sorting (first by country code, then by age)
  *     - this is implemented with Spark Core API. an alternative approach is to use DataFrame in the Spark SQL API
  */

// application specific config
object AppConf {
  // spark
  val appName         = "lolski-tremorvideo-problem1-part2"
  val masterUrl       = "local"
  val partitionCount  = 8

  // input
  val baseTmp         = s"${IO.getCwd}/files"
  val countryCodes    = s"$baseTmp/countryCodes.txt"
  val logs            = s"$baseTmp/logs"
  val out             = s"$baseTmp/out"
}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(AppConf.appName).setMaster(AppConf.masterUrl)
    val ctx  = new SparkContext(conf)

    // cache in master as map for constant time get operation
    val map = ctx.textFile(s"file:///${AppConf.countryCodes}")
      .map(parseCountryCodeMapEntry)
      .collectAsMap()

    // collect all logs
    val log = ctx.textFile(s"file:///${AppConf.logs}/*")
      .map { raw =>
        val user = parseLogEntry(raw)
        (user -> user.age)
      }

    val sorted = log repartitionAndSortWithinPartitions(
      partitioner = new PartitionByCountryCode(AppConf.partitionCount))

    val withFileName = sorted map { case (LogEntry(code, _), age) => (sanitizeCountryName(map(code)), age) }

    withFileName.saveAsHadoopFile(AppConf.out, classOf[Integer], classOf[String], classOf[KeyAsFileNameOutput[Integer, String]])

    ctx.stop()
  }
}