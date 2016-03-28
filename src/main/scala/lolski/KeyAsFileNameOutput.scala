package lolski

/**
  * Created by lolski on 3/28/16.
  */

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

class KeyAsFileNameOutput[T >: Null, V <: AnyRef] extends MultipleTextOutputFormat[T , V] {
  override def generateFileNameForKeyValue(key: T, value: V, leaf: String) = {
    key.toString + "/" + leaf
  }
  override protected def generateActualKey(key: T, value: V) = {
    null
  }
}
