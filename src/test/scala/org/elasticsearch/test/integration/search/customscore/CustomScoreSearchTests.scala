package org.elasticsearch.test.integration.search.customscore

import org.scalatest._, matchers._
import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.action.search._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) class CustomScoreSearchTests extends IndexerBasedTest {

  test("testCustomScriptBoost") {
    pending //fixme: failing test
    indexer.index(indexName, "type1", "1", """{"test": "value beck", "num1": 1.0}""")
    indexer.index(indexName, "type1", "2", """{"test": "value check", "num1": 2.0}""")
    indexer.refresh()
    //logger.info("--- QUERY_THEN_FETCH")
    //logger.info("running doc['num1'].value")
    var response = indexer.search(searchType = Some(SearchType.QUERY_THEN_FETCH), explain = Some(true), query = customScoreQuery(termQuery(indexName, "value")).script("doc['num1'].value"))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(1).id should be === ("1")
    //logger.info("running -doc['num1'].value")
    response = indexer.search(searchType = Some(SearchType.QUERY_THEN_FETCH), explain = Some(true), query = customScoreQuery(termQuery(indexName, "value")).script("-doc['num1'].value"))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(1).id should be === ("2")
    //logger.info("running pow(doc['num1'].value, 2)")
    response = indexer.search(searchType = Some(SearchType.QUERY_THEN_FETCH), explain = Some(true), query = customScoreQuery(termQuery(indexName, "value")).script("pow(doc['num1'].value, 2)"))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(1).id should be === ("1")
    //logger.info("running max(doc['num1'].value, 1)")
    response = indexer.search(searchType = Some(SearchType.QUERY_THEN_FETCH), explain = Some(true), query = customScoreQuery(termQuery(indexName, "value")).script("max(doc['num1'].value, 1d)"))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(1).id should be === ("1")
    //logger.info("running doc['num1'].value * _score")
    response = indexer.search(searchType = Some(SearchType.QUERY_THEN_FETCH), explain = Some(true), query = customScoreQuery(termQuery(indexName, "value")).script("doc['num1'].value * _score"))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(1).id should be === ("1")
    //logger.info("running param1 * param2 * _score")
    response = indexer.search(
      searchType = Some(SearchType.QUERY_THEN_FETCH), explain = Some(true),
      query = customScoreQuery(termQuery(indexName, "value")).script("param1 * param2 * _score").param("param1", 2).param("param2", 2))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(1).id should be === ("2")
  }

  test("testCustomFiltersScore") {
    indexer.index(indexName, "type", "1", """{"field": "value1", "color": "red"}""")
    indexer.index(indexName, "type", "2", """{"field": "value2", "color": "blue"}""")
    indexer.index(indexName, "type", "3", """{"field": "value3", "color": "red"}""")
    indexer.index(indexName, "type", "4", """{"field": "value4", "color": "blue"}""")
    indexer.refresh()

    var response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).add(termFilter("field",
      "value4"), "2")
      .add(termFilter("field", "value2"), "3"))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(0).score() should be === (3.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    Set("4") should contain(response.hits.getAt(1).id)
    response.hits.getAt(1).score() should be === (2.0)
    Set("1", "3") should contain(response.hits.getAt(2).id)
    response.hits.getAt(2).score() should be === (1.0)
    Set("1", "3") should contain(response.hits.getAt(3).id)
    response.hits.getAt(3).score() should be === (1.0)

    response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).add(termFilter("field",
      "value4"), 2)
      .add(termFilter("field", "value2"), 3))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("2")
    response.hits.getAt(0).score() should be === (3.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    Set("4") should contain(response.hits.getAt(1).id)
    response.hits.getAt(1).score() should be === (2.0)
    Set("1", "3") should contain(response.hits.getAt(2).id)
    response.hits.getAt(2).score() should be === (1.0)
    Set("1", "3") should contain(response.hits.getAt(3).id)
    response.hits.getAt(3).score() should be === (1.0)

    response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).scoreMode("total")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(0).score() should be === (8.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())

    response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).scoreMode("max")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(0).score() should be === (5.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())

    response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).scoreMode("avg")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("3")
    response.hits.getAt(0).score() should be === (5.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    response.hits.getAt(1).id should be === ("1")
    response.hits.getAt(1).score() should be === (4.0)
    //logger.info("--> Hit[1] {} Explanation {}", response.hits.getAt(1).id(), response.hits.getAt(1).explanation())

    response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).scoreMode("min")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("3")
    response.hits.getAt(0).score() should be === (5.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    response.hits.getAt(1).id should be === ("1")
    response.hits.getAt(1).score() should be === (3.0)
    response.hits.getAt(2).id should be === ("4")
    response.hits.getAt(2).score() should be === (2.0)
    response.hits.getAt(3).id should be === ("2")
    response.hits.getAt(3).score() should be === (1.0)

    response = indexer.search_prepare(Seq(indexName)).setQuery(customFiltersScoreQuery(matchAllQuery).scoreMode("multiply")
      .add(termFilter("field", "value4"), 2)
      .add(termFilter("field", "value1"), 3)
      .add(termFilter("color", "red"), 5))
      .setExplain(true).execute.actionGet
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (4)
    response.hits.getAt(0).id should be === ("1")
    response.hits.getAt(0).score() should be === (15.0)
    //logger.info("--> Hit[0] {} Explanation {}", response.hits.getAt(0).id(), response.hits.getAt(0).explanation())
    response.hits.getAt(1).id should be === ("3")
    response.hits.getAt(1).score() should be === (5.0)
    response.hits.getAt(2).id should be === ("4")
    response.hits.getAt(2).score() should be === (2.0)
    response.hits.getAt(3).id should be === ("2")
    response.hits.getAt(3).score() should be === (1.0)
  }
}
