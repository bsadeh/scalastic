package org.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.common.unit._

object Conversions {
  implicit object TimeValueOrdering extends Ordering[TimeValue] {
    def compare(first: TimeValue, second: TimeValue) = (first.millis - second.millis) toInt
  }

  implicit def toRichMultiSearchResponse(response: MultiSearchResponse) = new RichMultiSearchResponse(response)
}

class RichMultiSearchResponse(underlying: MultiSearchResponse) {
  def responses = underlying.responses map (_.response)
  def maxScores = responses map (_.hits.maxScore)
  def totalHits = responses map (_.hits.totalHits)
  def timeTook = responses map (_.took)
}