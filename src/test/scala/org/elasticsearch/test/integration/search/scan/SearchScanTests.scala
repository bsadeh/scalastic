package org.elasticsearch.test.integration.search.scan

import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.search._
import scala.collection.JavaConversions._
import scalastic.elasticsearch._
import collection.mutable

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SearchScanTests extends IndexerBasedTest {

  override def defaultSettings = Map("number_of_shards" -> "5")

  test("narrowingQuery") {
	val ids = new mutable.HashSet[String]
    val expectedIds = new mutable.HashSet[String]
    for (i <- 0 until 100) {
      expectedIds.add(i.toString)
      indexer.index(indexName, "tweet", i.toString, """{"user": "kimchy1", "postDate": %s, "message": "test"}""".format(System.currentTimeMillis()))
    }
    for (i <- 100 until 200) indexer.index(indexName, "tweet", i.toString, """{"user": "kimchy2", "postDate": %s, "message": "test"}""".format(System.currentTimeMillis()))
    indexer.refresh()
    var response = indexer.search(searchType = Some(SearchType.SCAN), query = termQuery("user", "kimchy1"), size = Some(35), scroll = Some("2m"))
    response.getHits.totalHits should be === (100)
    var continue = true
    while (continue) {
      response = indexer.searchScroll(response.getScrollId, scroll = Some("2m"))
      response.getHits.totalHits should be === (100)
      response.getFailedShards should be === (0)
      for (hit <- response.getHits) {
        ids.contains(hit.getId) should be === (false)
        ids.add(hit.getId)
      }
      continue = !response.getHits.hits.isEmpty
    }
    expectedIds should be === (ids)
  }
}
