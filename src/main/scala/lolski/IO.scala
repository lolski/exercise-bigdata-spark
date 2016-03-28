package lolski

/**
  * Created by lolski on 3/27/16.
  */

object IO {
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
}