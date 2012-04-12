package com.traackr.util

object Conversions {

  implicit def anyToMap(any: AnyRef) = toMap(any)

  def toMap(any: AnyRef): Map[String, AnyRef] = {
    any.getClass.getDeclaredFields.foldLeft(Map[String, AnyRef]()) {
      (result, each) =>
        each.setAccessible(true)
        result + (each.getName -> each.get(any))
    }
  }

}
