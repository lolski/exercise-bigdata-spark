package lolski

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lolski on 3/27/16.
  * Problem set 1, part 2
  * Main assumptions:
  *   - country code mapping is assumed to be static, small and needs to be accessed very frequently
  *
  */

case class InputRow(countryCode: String, age: Int)

object Main {

  // spark
  val appName   = "lolski-tremorvideo-problem1-part2"
  val masterUrl = "local"

  // input
  val tmp          = "/Users/lolski/Playground/tremorvideo-problem1-part2/in"
  val countryCodes = s"${tmp}/countryCodes.txt"
  val in           = s"${tmp}/logs/log0.txt"
  val out          = s"${tmp}/out.txt"

  def main(args: Array[String]): Unit = {
    import IO.parseToInputRow

    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    val ctx = new SparkContext(conf)
    val logs = Seq(in)
    val rdds = logs map { log => ctx.textFile(log).map(parseToInputRow) }

    rdds.foreach { e => e.foreach(println) }

    ctx.stop()
  }
}