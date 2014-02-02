package org.elasticsearch.test.integration.search.matchedfilters

import org.elasticsearch.index.query._, FilterBuilders._, QueryBuilders._
import scala.collection.JavaConversions._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class MatchedFiltersTests extends IndexerBasedTest {

  test("simpleMatchedFilter") {
    indexer.index(indexName, "type1", "1", """{"name": "test1", "number": 1}""")
    indexer.index(indexName, "type1", "2", """{"name": "test2", "number": 2}""")
    indexer.index(indexName, "type1", "3", """{"name": "test3", "number": 3}""")
    indexer.refresh()
    val response = indexer.search(query = filteredQuery(matchAllQuery, orFilter(rangeFilter("number").lte(2).filterName("test1"), rangeFilter("number").gt(2).filterName("test2"))))
    response.getHits.totalHits should equal (3)
    for (hit <- response.getHits) {
      if (hit.getId == "1" || hit.getId == "2")
        hit.matchedQueries should equal (Array("test1"))
      else if (hit.getId == "3")
        hit.matchedQueries should equal (Array("test2"))
      else 
        fail()
    }
  }
}
