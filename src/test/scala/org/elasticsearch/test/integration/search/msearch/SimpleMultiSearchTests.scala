package org.elasticsearch.test.integration.search.msearch

import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.scalatest._, matchers._
import com.traackr.scalastic.elasticsearch._

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
    response.responses().length should be === (3)
    response.responses()(0).response().hits.totalHits should be === (1)
    response.responses()(1).response().hits.totalHits should be === (1)
    response.responses()(2).response().hits.totalHits should be === (2)
    response.responses()(0).response().hits.getAt(0).id should be === ("1")
    response.responses()(1).response().hits.getAt(0).id should be === ("2")
  }
}
