package org.elasticsearch.test.integration.search.query

import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import org.elasticsearch.index.query._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleQueryTests extends IndexerBasedTest {

  test("passQueryAsStringTest") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1", "field2": "value2_1"}""", refresh = Some(true))
    indexer.refresh()
    indexer.search(query = termQuery("field1", "value1_1")).getHits.totalHits should be === (1)
  }

  test("queryStringAnalyzedWildcard") {
    indexer.index(indexName, "type1", "1", """{"field1": "value_1", "field2": "value_2"}""")
    indexer.refresh()
    indexer.search(query = queryString("value*").analyzeWildcard(true)).getHits.totalHits should be === (1)
    indexer.search(query = queryString("*ue*").analyzeWildcard(true)).getHits.totalHits should be === (1)
    indexer.search(query = queryString("*ue_1").analyzeWildcard(true)).getHits.totalHits should be === (1)
    indexer.search(query = queryString("val*e_1").analyzeWildcard(true)).getHits.totalHits should be === (1)
    indexer.search(query = queryString("v?l*e?1").analyzeWildcard(true)).getHits.totalHits should be === (1)
  }

  test("typeFilterTypeIndexedTests") {
    typeFilterTests("not_analyzed")
  }


  test("typeFilterTypeNotIndexedTests"){
    typeFilterTests("no")
  }

  private def typeFilterTests(index: String) {
    indexer.deleteIndex(Seq(indexName))
    indexer.createIndex(indexName,Map("index.number_of_shards" -> "1"))
    indexer.putMapping(indexName, "type1", """{"type1": {"_type": {"index": "%s"}}}""".format(index))
    indexer.putMapping(indexName, "type2", """{"type2": {"_type": {"index": "%s"}}}""".format(index))
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.index(indexName, "type2", "1", """{"field1": "value1"}""")
    indexer.flush()
    indexer.index(indexName, "type1", "2", """{"field1": "value1"}""")
    indexer.index(indexName, "type2", "2", """{"field1": "value1"}""")
    indexer.index(indexName, "type2", "3", """{"field1": "value1"}""")
    indexer.refresh()
    indexer.count(query = filteredQuery(matchAllQuery, typeFilter("type1"))).getCount should be === (2)
    indexer.count(query = filteredQuery(matchAllQuery, typeFilter("type2"))).getCount should be === (3)
    indexer.count(Nil, Seq("type1")).getCount should be === (2)
    indexer.count(Nil, Seq("type2")).getCount should be === (3)
    indexer.count(Nil, Seq("type1", "type2")).getCount should be === (5)
  }

  test("idsFilterTestsIdIndexed") {
    idsFilterTests("not_analyzed")
  }

  test("idsFilterTestsIdNotIndexed") {
    idsFilterTests("no")
  }

  private def idsFilterTests(index: String) {
    indexer.putMapping(indexName, "type1", """{"type1": {"_id": {"index": "%s"}}}""".format(index))
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.index(indexName, "type1", "2", """{"field1": "value2"}""")
    indexer.index(indexName, "type1", "3", """{"field1": "value3"}""")
    indexer.refresh()

    var response = indexer.search(query = constantScoreQuery(idsFilter("type1").ids("1", "3")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "3")

    response = indexer.search(query = constantScoreQuery(idsFilter().ids("1", "3")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "3")

    response = indexer.search(query = idsQuery("type1").ids("1", "3"))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "3")

    response = indexer.search(query = idsQuery().ids("1", "3"))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "3")

    response = indexer.search(query = idsQuery("type1").ids("7", "10"))
    response.getHits.totalHits should be === (0)
  }

  test("limitFilter_") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1"}""")
    indexer.index(indexName, "type1", "2", """{"field1": "value1_2"}""")
    indexer.index(indexName, "type1", "3", """{"field2": "value2_3"}""")
    indexer.index(indexName, "type1", "4", """{"field3": "value3_4"}""")
    indexer.refresh()
    val response = indexer.search(query = filteredQuery(matchAllQuery, limitFilter(2)))
    response.getHits.totalHits should be === (2)
  }

  test("filterExistsMissingTests") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1", "field2": "value2_1"}""")
    indexer.index(indexName, "type1", "2", """{"field1": "value1_2"}""")
    indexer.index(indexName, "type1", "3", """{"field2": "value2_3"}""")
    indexer.index(indexName, "type1", "4", """{"field3": "value3_4"}""")
    indexer.refresh()

    var response = indexer.search(query = filteredQuery(matchAllQuery, existsFilter("field1")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "2")

    response = indexer.search(query = constantScoreQuery(existsFilter("field1")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "2")

    response = indexer.search(query = queryString("_exists_:field1"))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "2")

    response = indexer.search(query = filteredQuery(matchAllQuery, existsFilter("field2")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("1", "3")

    response = indexer.search(query = filteredQuery(matchAllQuery, existsFilter("field3")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("4")

    response = indexer.search(query = filteredQuery(matchAllQuery, missingFilter("field1")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("3", "4")

    response = indexer.search(query = filteredQuery(matchAllQuery, missingFilter("field1")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("3", "4")

    response = indexer.search(query = constantScoreQuery(missingFilter("field1")))
    (response.getHits.hits() map (_.id)).toSet should be === Set("3", "4")

    response = indexer.search(query = queryString("_missing_:field1"))
    (response.getHits.hits() map (_.id)).toSet should be === Set("3", "4")
  }

  test("passQueryOrFilterAsJSONStringTest") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1_1", "field2": "value2_1"}""", refresh = Some(true))
    val wrapper = new WrapperQueryBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }")
    indexer.search(query = wrapper).getHits.totalHits should be === (1)
    
    val bool = new BoolQueryBuilder()
    bool.must(wrapper)
    bool.must(new TermQueryBuilder("field2", "value2_1"))
    indexer.search(query = wrapper).getHits.totalHits should be === (1)
    
    val wrapperFilter = new WrapperFilterBuilder("{ \"term\" : { \"field1\" : \"value1_1\" } }")
    indexer.search_prepare().setFilter(wrapperFilter).execute.actionGet.getHits.totalHits should be === (1)
  }

  test("filtersWithCustomCacheKey") {
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.refresh()

    var response = indexer.search(Seq(indexName), query = constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1")))
    response.getShardFailures.length should be === (0)
    response.getHits.totalHits should be === (1)

    response = indexer.search(Seq(indexName), query = constantScoreQuery(termsFilter("field1", "value1").cacheKey("test1")))
    response.getShardFailures.length should be === (0)
    response.getHits.totalHits should be === (1)

    response = indexer.search(Seq(indexName), query = constantScoreQuery(termsFilter("field1", "value1")))
    response.getShardFailures.length should be === (0)
    response.getHits.totalHits should be === (1)

    response = indexer.search(Seq(indexName), query = constantScoreQuery(termsFilter("field1", "value1")))
    response.getShardFailures.length should be === (0)
    response.getHits.totalHits should be === (1)
  }
}
