package com.traackr.elasticsearch

import org.elasticsearch.action.search.SearchType._
import org.elasticsearch.client.Requests._
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.builder.SearchSourceBuilder._
import org.scalatest._, matchers._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class CustomScoreSearchTest extends IndexerBasedTest {

  override def beforeEach = {
    super.beforeEach
    createDefaultIndex
  }

  test("custom script boost") {
    indexer.index(indexName, "type1", "1",
      jsonBuilder.startObject.field(indexName, "value beck").field("num1", 1.0).endObject.string)
    indexer.index(indexName, "type1", "2",
      jsonBuilder.startObject.field(indexName, "value check").field("num1", 2.0).endObject.string)
    indexer.refresh()
    logger.info("--- QUERY_THEN_FETCH")
    logger.info("running doc['num1'].value")
    var response = indexer.search(
      explain = true,
      searchType = QUERY_THEN_FETCH,
      query = customScoreQuery(termQuery("test", "value")).script("doc['num1'].value"))
    response.hits.totalHits should be === 2
    logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id, response.hits.getAt(0).explanation)
    logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id, response.hits.getAt(1).explanation)
    response.hits.getAt(0).id should be === "2"
    response.hits.getAt(1).id should be === "1"
    logger.info("running -doc['num1'].value")
    response = indexer.search(
      explain = true,
      searchType = QUERY_THEN_FETCH,
      query = customScoreQuery(termQuery("test", "value")).script("-doc['num1'].value"))
    response.hits.totalHits should be === 2
    logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id, response.hits.getAt(0).explanation)
    logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id, response.hits.getAt(1).explanation)
    response.hits.getAt(0).id should be === "1"
    response.hits.getAt(1).id should be === "2"
    logger.info("running pow(doc['num1'].value, 2)")
    response = indexer.search(
      explain = true,
      searchType = QUERY_THEN_FETCH,
      query = customScoreQuery(termQuery("test", "value")).script("pow(doc['num1'].value, 2)"))
    response.hits.totalHits should be === 2
    logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id, response.hits.getAt(0).explanation)
    logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id, response.hits.getAt(1).explanation)
    response.hits.getAt(0).id should be === "2"
    response.hits.getAt(1).id should be === "1"
    logger.info("running max(doc['num1'].value, 1)")
    response = indexer.search(
      explain = true,
      searchType = QUERY_THEN_FETCH,
      query = customScoreQuery(termQuery("test", "value")).script("max(doc['num1'].value, 1d)"))
    response.hits.totalHits should be === 2
    logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id, response.hits.getAt(0).explanation)
    logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id, response.hits.getAt(1).explanation)
    response.hits.getAt(0).id should be === "2"
    response.hits.getAt(1).id should be === "1"
    logger.info("running doc['num1'].value * _score")
    response = indexer.search(
      explain = true,
      searchType = QUERY_THEN_FETCH,
      query = customScoreQuery(termQuery("test", "value")).script("doc['num1'].value * _score"))
    response.hits.totalHits should be === 2
    logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id, response.hits.getAt(0).explanation)
    logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id, response.hits.getAt(1).explanation)
    response.hits.getAt(0).id should be === "2"
    response.hits.getAt(1).id should be === "1"
    logger.info("running param1 * param2 * _score")
    response = indexer.search(
      explain = true,
      searchType = QUERY_THEN_FETCH,
      query = customScoreQuery(termQuery("test", "value"))
        .script("param1 * param2 * _score")
        .param("param1", 2)
        .param("param2", 2))
    response.hits.totalHits should be === 2
    logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id, response.hits.getAt(0).explanation)
    logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id, response.hits.getAt(1).explanation)
    response.hits.getAt(0).id should be === "1"
    response.hits.getAt(1).id should be === "2"
  }

  test("custom filters score") {
    indexer.index(indexName, "type", "1", """{"field": "value1", "color": "red"}""")
    indexer.index(indexName, "type", "2", """{"field": "value2", "color": "blue"}""")
    indexer.index(indexName, "type", "3", """{"field": "value3", "color": "red"}""")
    indexer.index(indexName, "type", "4", """{"field": "value4", "color": "blue"}""")
    indexer.refresh()
    /*
    var response = indexer.search(indexName, customFiltersScoreQuery(matchAllQuery())
        .add(termFilter("field", "value4"), "2")
      .add(termFilter("field", "value2"), "3"))
      .setExplain(true)
    Arrays toString response.shardFailures(), response.failedShards() should be === 0
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "2"
    response.hits().getAt(0).score() should be === 3.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response.hits().getAt(1).id() should be === "4"
    response.hits().getAt(1).score() should be === 2.0
    response.hits().getAt(2).id(), anyOf(equalTo("1") should be === "3")
    response.hits().getAt(2).score() should be === 1.0
    response.hits().getAt(3).id(), anyOf(equalTo("1") should be === "3")
    response.hits().getAt(3).score() should be === 1.0
    response = prepareSearch(indexName).setQuery(customFiltersScoreQuery(matchAllQuery()).add(termFilter("field", 
      "value4"), 2)
      .add(termFilter("field", "value2"), 3))
      .setExplain(true)
      .execute()
      .actionGet()
    Arrays toString response.shardFailures(), response.failedShards() should be === 0
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "2"
    response.hits().getAt(0).score() should be === 3.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response.hits().getAt(1).id() should be === "4"
    response.hits().getAt(1).score() should be === 2.0
    response.hits().getAt(2).id(), anyOf(equalTo("1") should be === "3")
    response.hits().getAt(2).score() should be === 1.0
    response.hits().getAt(3).id(), anyOf(equalTo("1") should be === "3")
    response.hits().getAt(3).score() should be === 1.0
    response = prepareSearch(indexName).setQuery(customFiltersScoreQuery(matchAllQuery()).scoreMode("total")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true)
      .execute()
      .actionGet()
    Arrays toString response.shardFailures(), response.failedShards() should be === 0
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "1"
    response.hits().getAt(0).score() should be === 8.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response = prepareSearch(indexName).setQuery(customFiltersScoreQuery(matchAllQuery()).scoreMode("max")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true)
      .execute()
      .actionGet()
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "1"
    response.hits().getAt(0).score() should be === 5.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response = prepareSearch(indexName).setQuery(customFiltersScoreQuery(matchAllQuery()).scoreMode("avg")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true)
      .execute()
      .actionGet()
    Arrays toString response.shardFailures(), response.failedShards() should be === 0
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "3"
    response.hits().getAt(0).score() should be === 5.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response.hits().getAt(1).id() should be === "1"
    response.hits().getAt(1).score() should be === 4.0
    logger.info("--> Hit[1] {} Explanation {}", response.hits().getAt(1).id(), response.hits().getAt(1).explanation())
    response = prepareSearch(indexName).setQuery(customFiltersScoreQuery(matchAllQuery()).scoreMode("min")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true)
      .execute()
      .actionGet()
    Arrays toString response.shardFailures(), response.failedShards() should be === 0
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "3"
    response.hits().getAt(0).score() should be === 5.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response.hits().getAt(1).id() should be === "1"
    response.hits().getAt(1).score() should be === 3.0
    response.hits().getAt(2).id() should be === "4"
    response.hits().getAt(2).score() should be === 2.0
    response.hits().getAt(3).id() should be === "2"
    response.hits().getAt(3).score() should be === 1.0
    response = prepareSearch(indexName).setQuery(customFiltersScoreQuery(matchAllQuery()).scoreMode("multiply")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true)
      .execute()
      .actionGet()
    Arrays toString response.shardFailures(), response.failedShards() should be === 0
    response.hits().totalHits() should be === 4l
    response.hits().getAt(0).id() should be === "1"
    response.hits().getAt(0).score() should be === 15.0
    logger.info("--> Hit[0] {} Explanation {}", response.hits().getAt(0).id(), response.hits().getAt(0).explanation())
    response.hits().getAt(1).id() should be === "3"
    response.hits().getAt(1).score() should be === 5.0
    response.hits().getAt(2).id() should be === "4"
    response.hits().getAt(2).score() should be === 2.0
    response.hits().getAt(3).id() should be === "2"
    response.hits().getAt(3).score() should be === 1.0
     */
  }
}
