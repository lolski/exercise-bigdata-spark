package lolski

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lolski on 3/27/16.
  * Problem set 1, part 2
  * Main assumptions:
  *   - country code mapping is assumed to be static, small and needs to be accessed very frequently
  *
  */

object Main {
  case class InputRow(countryCode: String, age: Int)

  // spark
  val appName   = "lolski-tremorvideo-problem1-part2"
  val masterUrl = "local"

  // input
  val tmp         = "/Users/lolski/Playground/tremorvideo-problem1-part2/in"
  val countryCode = s"${tmp}/mapping.txt"
  val in          = s"${tmp}/logs/log0.txt"
  val out         = s"${tmp}/out.txt"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    val ctx = new SparkContext(conf)
    val logs = Seq(in)
    val rdds = logs map { log => ctx.textFile(log).map(parseToInputRow) }

    rdds.foreach { e => e.foreach(println) }

    ctx.stop()
  }

  def parseToInputRow(raw: String): InputRow = {
    // basic whitespace trimming before splitting on '&'
    val split = raw.replace(" ", "").split("&")

    // no error handling since the problem set assumes that inputs are always valid
    val parsed = split match {
      case Array(code, age) => InputRow(code, age.toInt)
    }

    parsed
  }

  def sanitizeCountryName(countryName: String) = {
    countryName.replaceAll("[^\\p{Alnum}\\s]", "")  // takes care of non-alphanumeric country name like 'Antigua & Barbuda'
      .replaceAll("\\s+", " ")                      // removes multiple subsequent white spaces
      .replaceAll(" ", "_")                         // replaces whitespace with underscore
      .toLowerCase                                  // lower case
  }
}
