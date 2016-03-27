package lolski

import java.io.{FileWriter, BufferedWriter, File}
import java.nio.file.{Paths, Files}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

import scala.annotation.tailrec

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

  // file writing helpers
  def withWriter[T](res: BufferedWriter)(block: BufferedWriter=> T): T = {
    try block(res)
    finally res.close()
  }

  def append2(dir: String, name: String, content: String) = {
    if (!Files.exists(Paths.get(dir))) Files.createDirectories(Paths.get(dir))
    IO.append(s"$dir/sorted.txt") { writer =>
      writer.write(content)
      writer.newLine()
    }
  }

  def append[T](path: String)(block: BufferedWriter => T): T = {
    withWriter(new BufferedWriter(new FileWriter(path, true))) { in =>
      block(in)
    }
  }

  def deleteDir(path: String): Unit = {
    def delete(file: File) {
      if (file.isDirectory)
        Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
      file.delete
    }

    val f = new File(path)
    delete(f)
  }
}