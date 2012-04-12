package com.traackr.util

import java.util.Properties
import java.io._

object Preamble {

  def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
    try { f(resource) } finally { resource.close }

}