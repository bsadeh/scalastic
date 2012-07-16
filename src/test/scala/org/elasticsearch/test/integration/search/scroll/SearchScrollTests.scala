package org.elasticsearch.test.integration.search.scroll

import org.elasticsearch.action.search._
import org.elasticsearch.common.unit._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.search.sort._
import scala.collection.JavaConversions._
import com.traackr.scalastic.elasticsearch._, SearchParameterTypes._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SearchScrollTests extends IndexerBasedTest {

  override def shouldCreateDefaultIndex = false

  test("simpleScrollQueryThenFetch") {
    indexer.createIndex(indexName, settings = Map("number_of_shards" -> "3"))
    for (i <- 0 until 100) indexer.index(indexName, "type1", i.toString, """{"field": %s}""".format(i))
    indexer.refresh()

    var counter = 0
    var response = indexer.search(size = Some(35), scroll = Some("2m"), sortings = Seq(FieldSort("field", order = SortOrder.ASC)))
    response.hits.getTotalHits should be === (100)
    response.hits.hits.length should be === (35)
    for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }

    response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
    response.hits.getTotalHits should be === (100)
    response.hits.hits.length should be === (35)
    for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }

    response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
    response.hits.getTotalHits should be === (100)
    response.hits.hits.length should be === (30)
    for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }
  }

  test("simpleScrollQueryThenFetchSmallSizeUnevenDistribution") {
    indexer.createIndex(indexName, settings = Map("number_of_shards" -> "3"))
    for (i <- 0 until 100) {
      val routing = if (i > 90) "1" else if (i > 60) "2" else "0"
      indexer.index(indexName, "type1", i.toString, """{"field": %s}""".format(i), routing = Some(routing))
    }
    indexer.refresh()

    var response = indexer.search_prepare().setSearchType(SearchType.QUERY_THEN_FETCH)
      .setQuery(matchAllQuery)
      .setSize(3)
      .setScroll(TimeValue.timeValueMinutes(2))
      .addSort("field", SortOrder.ASC).execute.actionGet
    var counter = 0
    response.hits.getTotalHits should be === (100)
    response.hits.hits.length should be === (3)
    for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }
    for (i <- 0 until 32) {
      response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
      response.hits.getTotalHits should be === (100)
      response.hits.hits.length should be === (3)
      for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }
    }
    response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
    response.hits.getTotalHits should be === (100)
    response.hits.hits.length should be === (1)
    for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }
    response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
    response.hits.getTotalHits should be === (100)
    response.hits.hits.length should be === (0)
    for (hit <- response.hits) { hit.sortValues()(0) should be === (counter); counter += 1 }
  }

  test("scrollAndUpdateIndex") {
    indexer.createIndex(indexName, settings = Map("number_of_shards" -> "5"))
    for (i <- 0 until 500) indexer.index(indexName, "tweet", i.toString, """{"user": "kimchy", "postDate": %s, "message": "test"}""".format(i))
    indexer.refresh()
    indexer.count().count should be === (500)
    indexer.count(query = termQuery("message", "test")).count should be === (500)
    indexer.count(query = termQuery("message", "test")).count should be === (500)
    indexer.count(query = termQuery("message", "update")).count should be === (0)
    indexer.count(query = termQuery("message", "update")).count should be === (0)
    var response = indexer.search(query = queryString("user:kimchy"), size = Some(35), scroll = Some("2m"), sortings = Seq(FieldSort("postDate", order = SortOrder.ASC)))
    do {
      for (searchHit <- response.hits.hits) {
        val map = searchHit.sourceAsMap() + ("message" -> "update")
        indexer.index(indexName, "tweet", searchHit.id(), jsonBuilder().map(map).string())
      }
      response = indexer.searchScroll(response.scrollId, scroll = Some("2m"))
    } while (response.hits.hits.length > 0);
    indexer.refresh()
    indexer.count().count should be === (500)
    indexer.count(query = termQuery("message", "test")).count should be === (0)
    indexer.count(query = termQuery("message", "test")).count should be === (0)
    indexer.count(query = termQuery("message", "update")).count should be === (500)
    indexer.count(query = termQuery("message", "update")).count should be === (500)
  }
}
