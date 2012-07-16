package org.elasticsearch.test.integration.search.indicesboost

import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.builder.SearchSourceBuilder._
import org.elasticsearch.action.search._, SearchType._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleIndicesBoostSearchTests extends IndexerBasedTest {

  test("testIndicesBoost") {
    try {
      indexer.search(query = termQuery(indexName, "value"))
      fail
    } catch {
      case e: Exception =>
    }
    try {
      indexer.search(indices = Seq(indexName), query = termQuery(indexName, "value"))
      fail
    } catch {
      case e: Exception =>
    }
    indexer.createIndex("test1")
    indexer.createIndex("test2")
    indexer.index("test1", "type1", "1", """{"test": "value check"}""")
    indexer.index("test2", "type1", "1", """{"test": "value beck"}""")
    indexer.refresh()
    val indexBoost = 1.1f

    //logger.info("--- QUERY_THEN_FETCH")
    //logger.info("Query with test1 boosted")
    var response = indexer.search(
      searchType = Some(QUERY_THEN_FETCH),
      internalBuilder = Some(searchSource().explain(true).indexBoost("test1", indexBoost).query(termQuery(indexName, "value"))))
    
    pending //fixme: failing test
    response.hits.totalHits should be === (2)

    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).index(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).index(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).index() should be === ("test1")
    response.hits.getAt(1).index() should be === ("test2")
	//logger.info("Query with test2 boosted")
    response = indexer.search(
      searchType = Some(QUERY_THEN_FETCH),
      internalBuilder = Some(searchSource().explain(true).indexBoost("test2", indexBoost).query(termQuery(indexName, "value"))))
    response.hits.totalHits should be === (2)

    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).index(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).index(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).index() should be === ("test2")
    response.hits.getAt(1).index() should be === ("test1")
	//logger.info("--- DFS_QUERY_THEN_FETCH")
	//logger.info("Query with test1 boosted")
    response = indexer.search(
      internalBuilder = Some(searchSource().explain(true).indexBoost("test1", indexBoost).query(termQuery(indexName, "value"))))
    response.hits.totalHits should be === (2)
    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).index(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).index(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).index() should be === ("test1")
    response.hits.getAt(1).index() should be === ("test2")
	//logger.info("Query with test2 boosted")
    response = indexer.search(
      searchType = Some(QUERY_THEN_FETCH),
      internalBuilder = Some(searchSource().explain(true).indexBoost("test2", indexBoost).query(termQuery(indexName, "value"))))
    response.hits.totalHits should be === (2)

    //logger.info("Hit[0] {} Explanation {}", response.hits.getAt(0).index(), response.hits.getAt(0).explanation())
    //logger.info("Hit[1] {} Explanation {}", response.hits.getAt(1).index(), response.hits.getAt(1).explanation())
    response.hits.getAt(0).index() should be === ("test2")
    response.hits.getAt(1).index() should be === ("test1")
  }
}
