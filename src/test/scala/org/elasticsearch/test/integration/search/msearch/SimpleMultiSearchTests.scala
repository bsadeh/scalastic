package org.elasticsearch.test.integration.search.msearch

import org.elasticsearch.index.query._, QueryBuilders._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleMultiSearchTests extends IndexerBasedTest {

  override def defaultSettings = Map("number_of_shards" -> "3", "number_of_replicas" -> "0")

  test("simpleMultiSearch") {
    indexer.index(indexName, "type", "1", """{"field": "xxx"}""")
    indexer.index(indexName, "type", "2", """{"field": "yyy"}""")
    indexer.refresh()
    val response = indexer.multisearch(Seq(
      indexer.search_prepare(indices = Seq(indexName), query = termQuery("field", "xxx")),
      indexer.search_prepare(indices = Seq(indexName), query = termQuery("field", "yyy")),
      indexer.search_prepare(indices = Seq(indexName))))
    response.getResponses.length should equal (3)
    response.getResponses()(0).getResponse.getHits.totalHits should equal (1)
    response.getResponses()(1).getResponse.getHits.totalHits should equal (1)
    response.getResponses()(2).getResponse.getHits.totalHits should equal (2)
    response.getResponses()(0).getResponse.getHits.getAt(0).id should equal ("1")
    response.getResponses()(1).getResponse.getHits.getAt(0).id should equal ("2")
  }
}
