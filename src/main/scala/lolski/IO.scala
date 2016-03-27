package lolski

import java.io.{FileWriter, BufferedWriter, File}
import java.nio.file.{Paths, Files}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
/**
  * Created by lolski on 3/27/16.
  */

object IO {
  class MultipleTextOutput[K, V](map: String => String) extends MultipleTextOutputFormat[K, V] {
    override protected def generateFileNameForKeyValue(key: K, value: V, name: String): String = map(key.toString)
  }

  def parseToCountryCodeMapping(raw: String): (String, String) = {
    // basic whitespace trimming before splitting on '&'
    val split = raw.split("=")

    // no error handling since the problem set assumes that inputs are always valid
    val parsed = split match {
      case Array(code, name) => (code -> name)
    }

    parsed
  }

  def parseToInputRow(raw: String): User = {
    // basic whitespace trimming before splitting on '&'
    val split = raw.replace(" ", "").split("&")

    // no error handling since the problem set assumes that inputs are always valid
    val parsed = split match {
      case Array(code, age) => User(code, age.toInt)
    }

    parsed
  }

  def sanitizeCountryName(countryName: String) = {
    countryName.replaceAll("[^\\p{Alnum}\\s]", "")  // takes care of non-alphanumeric country name like 'Antigua & Barbuda'
      .replaceAll("\\s+", " ")                      // removes multiple subsequent white spaces
      .replaceAll(" ", "_")                         // replaces whitespace with underscore
      .toLowerCase                                  // lower case
  }

  def writeSeq(numbers: Seq[String], out: String, shouldOverwrite: Boolean): String = {
    // pick overwrite or append function based on shouldOverwrite flag
    def fn[T]: String => (BufferedWriter => T) => T =
      if (shouldOverwrite) overwrite _ else append _

    fn(out) { writer =>
      numbers foreach { line =>
        writer.write(line)
        writer.newLine()
      }
    }

    out
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

  def delete(paths: Seq[String]): Unit = paths foreach { p => Files.delete(Paths.get(p))}
}