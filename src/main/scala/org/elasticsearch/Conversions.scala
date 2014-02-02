package org.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.common.unit._
import org.elasticsearch.common.settings._
import scala.collection._
import scala.language.implicitConversions

object Conversions {

  implicit class MapToSettings(val map: Map[String, String]) extends AnyVal {
    def toSettings = {
      val builder = ImmutableSettings.settingsBuilder
      for ((key, value) <- map) builder.put(key, value)
      builder.build
    }
  }

  implicit class RichMultiSearchResponse(val underlying: MultiSearchResponse) extends AnyVal {
    def responses = underlying.getResponses map (_.getResponse)
    def maxScores = responses map (_.getHits.maxScore)
    def totalHits = responses map (_.getHits.totalHits)
    def timeTook = responses map (_.getTook)
  }

  implicit object TimeValueOrdering extends Ordering[TimeValue] {
    def compare(first: TimeValue, second: TimeValue) = (first.millis - second.millis).toInt
  }

}

