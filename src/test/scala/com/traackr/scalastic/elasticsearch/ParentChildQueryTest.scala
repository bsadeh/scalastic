package com.traackr.scalastic.elasticsearch

import org.scalatest._, matchers._
import scalaz._, Scalaz._
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class ParentChildQueryTest extends IndexerBasedTest {

  override def beforeEach = {
    super.beforeEach
    indexData
  }

  test("normalSearch") {
    var response = indexer.search(query = filteredQuery(matchAllQuery, typeFilter("influencer")))
    response.hits.totalHits should be === 1
    response = indexer.search(indices = Seq(indexName), types = Seq("post"))
    response.hits.totalHits should be === 2
    response = indexer.search(query = queryString("content:Beautiful"), fields = List("*"))
    response.hits.totalHits should be === 1
    valueFor(response, 0, "content").toString should startWith("Beautiful")
  }

  test("topChildren sum query") {
    var response = indexer.search(indices = Seq(indexName), types = Seq("post"), query = queryString("Sausalito").defaultField("content"))
    response.hits.totalHits should be === 2
    val combninedScore = response.hits.getAt(0).getScore + response.hits.getAt(1).getScore
    // the sum of the two
    combninedScore should be === (0.53125)

    response = search(topChildrenQuery("post", textQuery("content", "Sausalito")).score("sum"))
    response.hits.totalHits should be === 1
    // the aggregated sum
    response.hits.getAt(0).getScore should be === (0.53125)
    response = search(hasChildQuery("post", textQuery("content", "Sausalito")))
    response = search(filteredQuery(matchAllQuery, hasChildFilter("post", textQuery("content", "Sausalito"))))
    response.hits.totalHits should be === 1
  }

  test("hasChild query search") {
    var response = indexer.search(
      query = hasChildQuery("post", textQuery("content", "Sausalito")),
      fields = List("name"))
    response.hits.totalHits should be === 1
    valueFor(response, 0, "name").toString should be === "John Doe"
    response = indexer.search(
      query = filteredQuery(matchAllQuery, hasChildFilter("post", textQuery("content", "Sausalito"))),
      fields = List("name"))
    response.hits.totalHits should be === 1
    valueFor(response, 0, "name").toString should be === "John Doe"
  }

  private def indexData = {
    val mapping =
      """
	{
	  "type":{
	    "_parent":{"type":"influencer"},
	    "properties":{
	      "content":{"type":"string", "store":"yes"},
	      "publishedDate":{"type":"date"}
	    }
	  }
	}
    """
    indexer.putMapping(indexName, "post", mapping)
    indexer.index(indexName, "influencer", "1", """{"name":"John Doe"}""")
    val properties1 = """
    {
	    "content":"@traackr to uncover who the leading voices at #CES2012 are: sn.im/ces2012 (#CESinfluence ), Sausalito",
	    "publishedDate":"2012-01-11T12:27:00"
    }
    """
    indexer.index(indexName, "post", "1", properties1, parent = "1")
    val properties2 = """
    {
	    "content":"Beautiful day in SF today: biked from Fisherman s Warf to Sausalito over the GG Bridge, and had lunch at Cafe Piccolo pic.twitter.com/vyhHOyAd",
	    "publishedDate":"2012-01-28T18:14:00"
    }
    """
    indexer.index(indexName, "post", "2", properties2, parent = "1")
    indexer.refresh()
  }
}
