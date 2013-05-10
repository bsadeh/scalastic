package org.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.common.unit._
import scala.language.implicitConversions

object Conversions {
  implicit object TimeValueOrdering extends Ordering[TimeValue] {
    def compare(first: TimeValue, second: TimeValue) = (first.millis - second.millis).toInt
  }

  implicit def toRichMultiSearchResponse(response: MultiSearchResponse) = new RichMultiSearchResponse(response)
}

class RichMultiSearchResponse(underlying: MultiSearchResponse) {
  def responses = underlying.getResponses map (_.getResponse)
  def maxScores = responses map (_.getHits.maxScore)
  def totalHits = responses map (_.getHits.totalHits)
  def timeTook = responses map (_.getTook)
}