package org.elasticsearch

import org.elasticsearch.action.search._
import org.elasticsearch.common.unit._

object Conversions {
  implicit def toRichMultiSearchResponse(response: MultiSearchResponse) = new RichMultiSearchResponse(response)

  implicit object TimeValueOrdering extends Ordering[TimeValue] {
    def compare(first: TimeValue, second: TimeValue) = (first.millis - second.millis) toInt
  }

  class RichMultiSearchResponse(underlying: MultiSearchResponse) {
    def responses = underlying.responses map (_.response)
    def maxScores = responses map (_.hits.maxScore)
    def totalHits = responses map (_.hits.totalHits)
    def took = responses map (_.took)
    def maxTime = took.max
    def minTime = took.min
  }
}