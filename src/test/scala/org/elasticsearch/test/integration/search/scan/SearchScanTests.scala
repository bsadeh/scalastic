package org.elasticsearch.test.integration.search.scan

import org.scalatest._, matchers._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.search._
import scala.collection.JavaConversions._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SearchScanTests extends IndexerBasedTest {

  override def defaultSettings = Map("number_of_shards" -> "5")

  test("narrowingQuery") {
	import scala.collection.mutable._
    var ids = new HashSet[String]
    var expectedIds = new HashSet[String]
    for (i <- 0 until 100) {
      expectedIds.add(i.toString)
      indexer.index(indexName, "tweet", i.toString, """{"user": "kimchy1", "postDate": %s, "message": "test"}""".format(System.currentTimeMillis()))
    }
    for (i <- 100 until 200) indexer.index(indexName, "tweet", i.toString, """{"user": "kimchy2", "postDate": %s, "message": "test"}""".format(System.currentTimeMillis()))
    indexer.refresh()
    var response = indexer.search(searchType = Some(SearchType.SCAN), query = termQuery("user", "kimchy1"), size = Some(35), scroll = Some("2m"))
    response.hits.totalHits should be === (100)
    var continue = true
    while (continue) {
      response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
      response.hits.totalHits should be === (100)
      response.failedShards() should be === (0)
      for (hit <- response.hits) {
        ids.contains(hit.id()) should be === (false)
        ids.add(hit.id())
      }
      continue = !response.hits.hits.isEmpty
    }
    expectedIds should be === (ids)
  }
}
