package lolski

import java.io.File

/**
  * Created by lolski on 3/27/16.
  */

object IO {
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
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