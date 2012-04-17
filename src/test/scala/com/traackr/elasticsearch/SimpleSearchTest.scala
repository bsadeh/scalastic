package com.traackr.elasticsearch

import org.scalatest._, matchers._
import scalaz._, Scalaz._
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.QueryBuilders._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleSearchTest extends IndexerBasedTest {

  override def beforeEach {
    super.beforeEach
    createDefaultIndex
  }

  test("text search") {
    indexer.index(indexName, "type1", "1", """{"field": "trying to find an exact match"}""")
    indexer.index(indexName, "type1", "2", """{"field": "trying to find, but failing to find, an exact match"}""")
    indexer.index(indexName, "type1", "3", """{"field": "don't try this at home!"}""")
    indexer.refresh()

    search("field:bla").hits.totalHits should be === 0
    search("field:try").hits.totalHits should be === 1
    search("trying").hits.totalHits should be === 2
    search("trying to find an exact match").hits.totalHits should be === 2
    search("\"trying to find an exact match\"").hits.totalHits should be === 1

//fixme: these fail in maven
//    search(textPhraseQuery("field", "trying to find an exact match")).hits.totalHits should be === 1
//    search(textPhraseQuery("field", "\"trying to find an exact match\"")).hits.totalHits should be === 1
  }

  test("ip tests") {
    val mapping = """
    {
	  "type1": {
	    "properties" : {
			"from" : {"type": "ip"},
    		"to" : {"type": "ip"}		
	    }
	  }
    }
    """
    indexer.putMapping(indexName, "type9", mapping)
    indexer.index(indexName, "type9", "1", """{"from":"192.168.0.5", "to":"192.168.0.10"}""")
    indexer.refresh()
    val response = indexer.search(query = boolQuery
      .must(rangeQuery("from") lt "192.168.0.7")
      .must(rangeQuery("to") gt "192.168.0.7"))
    response.hits.totalHits should be === 1
  }

  test("id tests") {
    indexer.index(indexName, "type", "XXX1", """{"field":"value"}""")
    indexer.refresh()
    search(termQuery("_id", "XXX1")).hits.totalHits should equal(1)
    search(queryString("_id:XXX1")).hits.totalHits should be === 1
    search(prefixQuery("_id", "XXX")).hits.totalHits should be === 1
    search(queryString("_id:XXX*")).hits.totalHits should be === 1
  }

  test("dateMath") {
    indexer.index(indexName, "type1", "1", """{"field":"2010-01-05T02:00"}""")
    indexer.index(indexName, "type1", "2", """{"field":"2010-01-06T02:00"}""")
    indexer.refresh()
    indexer.search(query = rangeQuery("field").gte("2010-01-03||+2d").lte("2010-01-04||+2d")).hits.totalHits should be === 2
    indexer.search(query = queryString("field:[2010-01-03||+2d TO 2010-01-04||+2d]")).hits.totalHits should be === 2
  }

  test("multiSearch") {
    indexer.index(indexName, "type", "1", """{"field":"xxx"}""")
    indexer.index(indexName, "type", "2", """{"field":"yyy"}""")
    indexer.refresh()
    val queries = List(termQuery("field", "xxx"), termQuery("field", "yyy"), matchAllQuery)
    val responses = indexer.multisearch(queries).responses
    responses.length should be === 3
    responses(0).response.hits.totalHits should be === 1
    responses(0).response.hits.getAt(0).id should be === "1"
    responses(1).response.hits.totalHits should be === 1
    responses(1).response.hits.getAt(0).id should be === "2"
    responses(2).response.hits.totalHits should be === 2
  }

  test("dateRange with upper inclusive enabled") {
    indexer.index(indexName, "type1", "1", """{"field":"2010-01-05T02:00"}""")
    indexer.index(indexName, "type1", "2", """{"field":"2010-01-06T02:00"}""")
    indexer.refresh()
    search(rangeQuery("field").gte("2010-01-05").lte("2010-01-06")).hits.totalHits should be === 2
    search(rangeQuery("field").gte("2010-01-05") lt "2010-01-06").hits.totalHits should be === 1
  }
  
  test("dateRange with upper inclusive disabled") {
    val specialIndex = indexName + "_with_upper_inclusive_disabled"
    indexer.createIndex(specialIndex, settings = "{\"index.mapping.date.parse_upper_inclusive\":false}")
    indexer.index(specialIndex, "type1", "1", """{"field":"2010-01-05T02:00"}""")
    indexer.index(specialIndex, "type1", "2", """{"field":"2010-01-06T02:00"}""")
    indexer.refresh()
    indexer.search(indices=List(specialIndex), query = rangeQuery("field").gte("2010-01-05").lte("2010-01-06")).hits.totalHits should be === 1
    indexer.search(indices=List(specialIndex), query = rangeQuery("field").gte("2010-01-05") lt "2010-01-06").hits.totalHits should be === 1
  }
}
