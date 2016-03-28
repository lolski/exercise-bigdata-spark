package lolski

/**
  * Created by lolski on 3/27/16.
  */

object LogEntry {
  implicit def ordering[T <: LogEntry]: Ordering[T] = {
    Ordering.by(k => (k.countryCode, k.age))
  }
}

case class LogEntry(countryCode: String, age: Int)
