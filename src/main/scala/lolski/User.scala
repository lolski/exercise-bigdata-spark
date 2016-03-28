package lolski

/**
  * Created by lolski on 3/27/16.
  */

object User {
  implicit def ordering[T <: User]: Ordering[T] = {
    Ordering.by(k => (k.countryCode, k.age))
  }
}

case class User(countryCode: String, age: Int)
