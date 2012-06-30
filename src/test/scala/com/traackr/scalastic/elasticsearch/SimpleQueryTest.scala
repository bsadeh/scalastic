package com.traackr.scalastic.elasticsearch

import org.scalatest._, matchers._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.search._
import org.elasticsearch.index.query._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleQueryTest extends IndexerBasedTest {

  override def beforeEach {
    super.beforeEach
    createDefaultIndex
  }

  test("pass query as string") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1", "field2": "value2_1"}""")
    indexer.refresh()
    indexer.query("""{"term" : {"field1" : "value1_1"}}""").hits.totalHits should be === (1)
  }

  test("queryString analyzedWildcard") {
    indexer.index(indexName, "type1", "1", """{"field1": "value_1", "field2": "value_2"}""")
    indexer.refresh()
    indexer.search(query = queryString("value*").analyzeWildcard(true)).hits.totalHits should be === (1)
    indexer.search(query = queryString("*ue*").analyzeWildcard(true)).hits.totalHits should be === (1)
    indexer.search(query = queryString("*ue_1").analyzeWildcard(true)).hits.totalHits should be === (1)
    indexer.search(query = queryString("val*e_1").analyzeWildcard(true)).hits.totalHits should be === (1)
    indexer.search(query = queryString("v?l*e?1").analyzeWildcard(true)).hits.totalHits should be === (1)
  }

  test("typeFilter type indexed") {
    typeFilterTests("not_analyzed")
  }

  test("typeFilter type not indexed") {
    typeFilterTests("no")
  }

  private def typeFilterTests(indexValue: String) {
    indexer.putMapping(indexName, "type1", """{"_type":{"index": "%s"}}""".format(indexValue))
    indexer.putMapping(indexName, "type2", """{"_type":{"index": "%s"}}""".format(indexValue))
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.index(indexName, "type2", "1", """{"field1": "value1"}""")
    indexer.index(indexName, "type1", "2", """{"field1": "value1"}""")
    indexer.index(indexName, "type2", "2", """{"field1": "value1"}""")
    indexer.index(indexName, "type2", "3", """{"field1": "value1"}""")
    indexer.refresh()
    
    indexer.count(query = filteredQuery(matchAllQuery, typeFilter("type1"))).count should be === (2)
    indexer.count(query = filteredQuery(matchAllQuery, typeFilter("type2"))).count should be === (3)
    indexer.count(types = Seq("type1")).count should be === (2)
    indexer.count(types = Seq("type2")).count should be === (3)
    indexer.count(types = Seq("type1", "type2")).count should be === (5)
  }

  test("limitFilter") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1"}""")
    indexer.index(indexName, "type1", "2", """{"field1": "value1_2"}""")
    indexer.index(indexName, "type1", "3", """{"field2": "value2_3"}""")
    indexer.index(indexName, "type1", "4", """{"field3": "value3_4"}""")
    indexer.refresh()
    indexer.search().hits.totalHits should be === (4)
    indexer.search(query = filteredQuery(matchAllQuery, limitFilter(2))).hits.totalHits should be === (2)
  }

  test("pass query as json string") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1", "field2": "value2_1"}""")
    indexer.refresh()
    val wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }")
    var response = indexer.search(query = wrapper)
    response.hits.totalHits should be === (1)

    val bool = new BoolQueryBuilder
    bool.must(wrapper)
    bool.must(new TermQueryBuilder("field2", "value2_1"))
    response = indexer.search(query = wrapper)
    response.hits.totalHits should be === (1)
  }

  test("filters with custom cacheKey") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.refresh()

    var response = indexer.search(query = constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1")))
    response.shardFailures.length should be === (0)
    response.hits.totalHits should be === (1)

    response = indexer.search(query = constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1")))
    response.shardFailures.length should be === (0)
    response.hits.totalHits should be === (1)

    response = indexer.search(query = constantScoreQuery(termsFilter("field1", "value1")))
    response.shardFailures.length should be === (0)
    response.hits.totalHits should be === (1)

    response = indexer.search(query = constantScoreQuery(termsFilter("field1", "value1")))
    response.shardFailures.length should be === (0)
    response.hits.totalHits should be === (1)
  }
}
