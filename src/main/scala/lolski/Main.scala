package lolski

import org.apache.spark.{SparkConf, SparkContext}
import IO.{parseToInputRow, parseToCountryCodeMapping, sanitizeCountryName}

/**
  * Created by lolski on 3/27/16.
  * Problem set 1, part 2
  * Main assumptions:
  *   - using basic spark configuration for local run in a single machine
  *   - country code mapping is assumed to be static, small and needs to be accessed very frequently
  *   - log files are large and may not fit in memory
  * Implementation:
  *   - make a program that supports secondary sorting (first by country code, then by age)
  *     - this is implemented with Spark Core API. an alternative approach is to use DataFrame in the Spark SQL API
  */

object Main {
  // spark
  val appName   = "lolski-tremorvideo-problem1-part2"
  val masterUrl = "local"

  // input
  val tmp          = "/Users/lolski/Playground/tremorvideo-problem1-part2/in"
  val countryCodes = s"$tmp/countryCodes.txt"
  val logs         = s"$tmp/logs"
  val out          = s"$tmp/out"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    val ctx  = new SparkContext(conf)

    // cache as map for constant time get operation
    val map = ctx.textFile(s"file:///$countryCodes")
                 .map(parseToCountryCodeMapping)
                 .collectAsMap()

    val logRdd = ctx.textFile(s"file:///$logs/*").map(parseToInputRow) // collect all logs

    val sortedRdd = logRdd sortBy { e => (e.countryCode, e.age) } // sort by country code first, then age as the secondary field

    // remove output
    
    sortedRdd foreach { e =>
      val path = sanitizeCountryName(map(e.countryCode))
      IO.append(path) { writer =>
        writer.write(e.age)
        writer.newLine()
      }
    }
    ctx.stop()
  }
}