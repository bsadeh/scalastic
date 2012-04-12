package com.traackr.util

import java.util._
import java.io._

case class Props(filename: String) {
  val properties = new Properties()
  if (new File(filename).exists())
    properties.load(new FileReader(filename))
  else
    properties.load(ClassLoader.getSystemResourceAsStream(filename))

  def apply(key: String): String = {
    properties.getProperty(key)
  }

  def update(key: String, value: String) = {
    properties.put(key, value)
  }

  override def toString() = {
    properties.toString()
  }
}