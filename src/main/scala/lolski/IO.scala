package lolski

import java.io.{FileWriter, BufferedWriter}
import java.nio.file.{Files, Paths}

/**
  * Created by lolski on 3/27/16.
  */

object IO {
  object LogGenerator {
    def random(countryCodes: Vector[String], maxAge: Int = 130): LogEntry = {
      val idx = Math.abs(scala.util.Random.nextInt())
      val age = Math.abs(scala.util.Random.nextInt(maxAge))
      val code = countryCodes(idx % countryCodes.size)
      LogEntry(code, age)
    }
  }

  def writeLog(countryCodes: scala.collection.immutable.Set[String], numOfLogFiles: Int, linesPerLogFile: Int, outDir: String): Unit = {
    val countryCodesVec = countryCodes.toVector

    (1 to numOfLogFiles) foreach { i =>
      overwrite(s"$outDir/log$i.txt") { writer =>
        (1 to linesPerLogFile) foreach { _ =>
          val entry = LogGenerator.random(countryCodesVec)
          writer.write(s"${entry.countryCode}&${entry.age}")
          writer.newLine()
        }
      }
    }
  }

  def parseCountryCodeMapEntry(raw: String): (String, String) = {
    // basic whitespace trimming before splitting on '&'
    val split = raw.split("=")

    // no error handling since the problem set assumes that inputs are always valid
    val parsed = split match {
      case Array(code, name) => (code -> name)
    }

    parsed
  }

  def parseLogEntry(raw: String): LogEntry = {
    // basic whitespace trimming before splitting on '&'
    val split = raw.replace(" ", "").split("&")

    LogEntry(countryCode = split(0), age = split(1).toInt)
  }

  def sanitizeCountryName(countryName: String) = {
    countryName.replaceAll("[^\\p{Alnum}\\s]", "")  // takes care of non-alphanumeric country name like 'Antigua & Barbuda'
      .replaceAll("\\s+", " ")                      // removes multiple subsequent white spaces
      .replaceAll(" ", "_")                         // replaces whitespace with underscore
      .toLowerCase                                  // lower case
  }

  // file writing helpers
  def overwrite[T](path: String)(block: BufferedWriter => T): T = {
    Files.deleteIfExists(Paths.get(path))
    append(path) { in =>
      block(in)
    }
  }

  def append[T](path: String)(block: BufferedWriter => T): T = {
    withWriter(new BufferedWriter(new FileWriter(path, true))) { in =>
      block(in)
    }
  }

  def withWriter[T](res: BufferedWriter)(block: BufferedWriter=> T): T = {
    try block(res)
    finally res.close()
  }

  def getCwd = Paths.get("").toAbsolutePath.toString
}