package org.elasticsearch.test.integration.search.basic

import org.elasticsearch._
import org.elasticsearch.action.search._, SearchType._
import org.elasticsearch.client.Requests._
import org.elasticsearch.common._, xcontent._, XContentFactory._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.builder.SearchSourceBuilder._
import org.elasticsearch.search.facet._
import org.elasticsearch.search.facet.query._
import org.elasticsearch.search.sort._
import scala.collection._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class TransportTwoNodesSearchTests extends MultiNodesBasedTests {
  var fullExpectedIds = new mutable.HashSet[String]

  override def beforeAll() {
    startNode("server1")
    startNode("server2")
  }

  override def beforeEach {
    super.beforeEach
    indexer("server2").createIndex(indexName, settings = Map("numberOfReplicas" -> "0", "numberOfShards" -> "3", "routing.hash.type" -> "simple"))
    indexer("server1").waitForGreenStatus()
    fullExpectedIds.clear()
    for (i <- 0 until 100) {
      val id = i.toString
      indexer("server1").index(indexName, "type1", id, source = source(id, "test", i).string)
      fullExpectedIds.add(i.toString)
    }
    indexer("server1").refresh()
  }

  test("testDfsQueryThenFetch") {
    val source = searchSource().query(termQuery("multi", "test")).from(0)
      .size(60)
      .explain(true)
    var response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source), scroll = Some("10m"))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (60)
    response.getHits.hits().map(_.id).toSet should equal (((for (i <- 0 until 60) yield (100 - 1 - i).toString)).toSet)

    response = indexer("server1").searchScroll(response.getScrollId)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    response.getHits.hits().map(_.id).toSet should equal (((for (i <- 0 until 40) yield (40 - 1 - i).toString)).toSet)
  }

  test("testDfsQueryThenFetchWithSort") {
    val source = searchSource().query(termQuery("multi", "test")).from(0)
      .size(60)
      .explain(true)
      .sort("age", SortOrder.ASC)
    var response = indexer("server1").search(Seq(indexName), searchType = Some(DFS_QUERY_THEN_FETCH), internalBuilder = Some(source), scroll = Some("10m"))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (60)
    for (i <- 0 until 60) {
      val hit = response.getHits.hits()(i)
      hit.explanation() should not be (null)
      hit.id should equal ((i.toString))
    }
    response = indexer("server1").searchScroll(response.getScrollId)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    for (i <- 0 until 40) {
      val hit = response.getHits.hits()(i)
      hit.id should equal ((i + 60).toString)
    }
  }

  test("testQueryThenFetch") {
    val source = searchSource().query(termQuery("multi", "test")).sort("nid", SortOrder.DESC)
      .from(0)
      .size(60)
      .explain(true)
    var response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source), scroll = Some("10m"))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (60)
    for (i <- 0 until 60) {
      val hit = response.getHits.hits()(i)
      hit.explanation() should not be (null)
      hit.id should equal ((100 - i - 1).toString)
    }
    response = indexer("server1").searchScroll(response.getScrollId)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    for (i <- 0 until 40) {
      val hit = response.getHits.hits()(i)
      hit.id should equal ((40 - 1 - i).toString)
    }
  }

  test("testQueryThenFetchWithFrom") {
    val source = searchSource().query(matchAllQuery).explain(true)
    val collectedIds = new mutable.HashSet[String]()
    var response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source.from(60).size(60)))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    pending //fixme: failing test
    response.getHits.getHits.length should equal (60)
    for (i <- 0 until 60) {
      val hit = response.getHits.hits()(i)
      collectedIds.add(hit.id())
    }
    response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source.from(60).size(60)))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    for (i <- 0 until 40) {
      val hit = response.getHits.hits()(i)
      collectedIds.add(hit.id())
    }
    collectedIds should equal (fullExpectedIds)
  }

  test("testQueryThenFetchWithSort") {
    val source = searchSource().query(termQuery("multi", "test")).from(0)
      .size(60)
      .explain(true)
      .sort("age", SortOrder.ASC)
    var response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source), scroll = Some("10m"))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (60)
    for (i <- 0 until 60) {
      val hit = response.getHits.hits()(i)
      hit.explanation() should not be (null)
      hit.id should equal ((i.toString))
    }
    response = indexer("server1").searchScroll(response.getScrollId)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    for (i <- 0 until 40) {
      val hit = response.getHits.hits()(i)
      hit.id should equal ((i + 60).toString)
    }
  }

  test("testQueryAndFetch") {
    val source = searchSource().query(termQuery("multi", "test")).from(0)
      .size(20)
      .explain(true)
    val expectedIds = new mutable.HashSet[String]()
    for (i <- 0 until 100) {
      expectedIds.add("" + i)
    }
    var response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source), scroll = Some("10m"))
    response.getShardFailures.length should equal (0)
    pending //fixme: failing test
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (60)
    for (i <- 0 until 60) expectedIds.remove(response.getHits.hits()(i).id)

    response = indexer("server1").searchScroll(response.getScrollId)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    for (i <- 0 until 40) expectedIds.remove(response.getHits.hits()(i).id)
    expectedIds.size should equal (0)
  }

  test("testDfsQueryAndFetch") {
    val source = searchSource().query(termQuery("multi", "test")).from(0)
      .size(20)
      .explain(true)
    val expectedIds = new mutable.HashSet[String]()
    for (i <- 0 until 100) {
      expectedIds.add("" + i)
    }
    var response = indexer("server1").search(Seq(indexName), searchType = Some(DFS_QUERY_THEN_FETCH), internalBuilder = Some(source), scroll = Some("10m"))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    pending //fixme: failing test
    response.getHits.getHits.length should equal (60)
    for (i <- 0 until 60) {
      val hit = response.getHits.hits()(i)
      hit.explanation() should not be (null)
      expectedIds.remove(hit.id)
    }
    response = indexer("server1").searchScroll(response.getScrollId)
    response.getHits.totalHits should equal (100)
    response.getHits.getHits.length should equal (40)
    for (i <- 0 until 40) expectedIds.remove(response.getHits.hits()(i).id)
    expectedIds.size should equal (0)
  }

  def testSimpleFacets() {
    val sourceBuilder = searchSource().query(termQuery("multi", "test")).from(0)
      .size(20)
      .explain(true)
      .facet(FacetBuilders.queryFacet("all", termQuery("multi", "test"))
        .global(true))
      .facet(FacetBuilders.queryFacet("test1", termQuery("name", "test1")))
    var response = indexer("server1").search(Seq(indexName), internalBuilder = Some(sourceBuilder))
    response.getShardFailures.length should equal (0)
    response.getHits.totalHits should equal (100)
    response.getFacets.facet(classOf[QueryFacet], "test1").getCount should equal (1)
    response.getFacets.facet(classOf[QueryFacet], "all").getCount should equal (100)
  }

  test("testSimpleFacetsTwice") {
    testSimpleFacets()
    testSimpleFacets()
  }

  test("testFailedSearchWithWrongQuery") {
    try {
      val response = indexer("server1").client.search(searchRequest(indexName).source(Strings.toUTF8Bytes("{ xxx }"))).actionGet
      response.getTotalShards should equal (3)
      response.getSuccessfulShards should equal (0)
      response.getFailedShards should equal (3)
      fail("search should fail")
    } catch {
      case e: ElasticsearchException => e.unwrapCause().getClass should equal (classOf[SearchPhaseExecutionException])
    }
  }

  test("testFailedSearchWithWrongFrom") {
    val source = searchSource().query(termQuery("multi", "test")).from(1000).size(20).explain(true)
    var response = indexer("server1").search(Seq(indexName), searchType = Some(DFS_QUERY_AND_FETCH), internalBuilder = Some(source))
    response.getHits.getHits.length should equal (0)
    response.getTotalShards should equal (3)
    response.getSuccessfulShards should equal (3)
    response.getFailedShards should equal (0)
    response = indexer("server1").search(Seq(indexName), searchType = Some(QUERY_THEN_FETCH), internalBuilder = Some(source))
    response.getShardFailures.length should equal (0)
    response.getHits.getHits.length should equal (0)
    response = indexer("server1").search(Seq(indexName), searchType = Some(DFS_QUERY_AND_FETCH), internalBuilder = Some(source))
    response.getShardFailures.length should equal (0)
    response.getHits.getHits.length should equal (0)
    response = indexer("server1").search(Seq(indexName), searchType = Some(DFS_QUERY_THEN_FETCH), internalBuilder = Some(source))
    response.getShardFailures.length should equal (0)
    response.getHits.getHits.length should equal (0)
  }

  private def source(id: String, nameValue: String, age: Int): XContentBuilder = {
    val multi = new StringBuilder().append(nameValue)
    for (i <- 0 until age) {
      multi.append(" ").append(nameValue)
    }
    jsonBuilder().startObject().field("id", id).field("nid", Integer.parseInt(id))
      .field("name", nameValue + id)
      .field("age", age)
      .field("multi", multi.toString())
      .field("_boost", age * 10)
      .endObject()
  }
}
