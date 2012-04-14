package com.traackr.elasticsearch

import org.scalatest._, matchers._
import scalaz._, Scalaz._
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.FilterBuilders._
import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.search.facet._, terms._, FacetBuilders._
import org.elasticsearch.action.search._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleChildQuerySearchTest extends IndexerBasedTest {

  override def beforeEach {
    super.beforeEach
    createDefaultIndex
  }

  test("multilevel child") {
    indexer.putMapping(indexName, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.putMapping(indexName, "grandchild", """{"type" : {"_parent" : {"type" : "child"}}}""")
    indexer.index(indexName, "parent", "p1", """{"p_field" : "p_value1"}""")
    indexer.index(indexName, "child", "c1", """{"c_field" : "c_value1"}""", parent = "p1".some)
    indexer.index(indexName, "grandchild", "gc1", """{"gc_field": "gc_value1"}""", parent = "c1".some, routing = "gc1".some)
    indexer.refresh()
    val filtered = filteredQuery(
      matchAllQuery,
      hasChildFilter("child", filteredQuery(termQuery("c_field", "c_value1"),
        hasChildFilter("grandchild", termQuery("gc_field", "gc_value1")))))
    val response = indexer.search(query = filtered)
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
  }

  test("simple child query") {
    indexer.putMapping(indexName, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.index(indexName, "parent", "p1", """{"p_field" :  "p_value1"}""")
    indexer.index(indexName, "child", "c1", """{"c_field" : "red"}""", parent = "p1".some)
    indexer.index(indexName, "child", "c2", """{"c_field" : "yellow"}""", parent = "p1".some)
    indexer.index(indexName, "parent", "p2", """{"p_field" : "p_value2"}""")
    indexer.index(indexName, "child", "c3", """{"c_field" : "blue"}""", parent = "p2".some)
    indexer.index(indexName, "child", "c4", """{"c_field" : "red"}""", parent = "p2".some)
    indexer.refresh()
    var response = indexer.search(query = idsQuery("child").ids("c1"), fields = Seq("_parent"))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("c1")
    response.hits.getAt(0).field("_parent").value.toString should be === ("p1")

    response = indexer.search(query = termQuery("child._parent", "p1"), fields = Seq("_parent"))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("c1", "c2") should contain(response.hits.getAt(0).id)
    response.hits.getAt(0).field("_parent").value.toString should be === ("p1")
    Set("c1", "c2") should contain(response.hits.getAt(1).id)
    response.hits.getAt(1).field("_parent").value.toString should be === ("p1")

    response = indexer.search(query = termQuery("_parent", "p1"), fields = Seq("_parent"))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("c1", "c2") should contain(response.hits.getAt(0).id)
    response.hits.getAt(0).field("_parent").value.toString should be === ("p1")
    Set("c1", "c2") should contain(response.hits.getAt(1).id)
    response.hits.getAt(1).field("_parent").value.toString should be === ("p1")

    response = indexer.search(query = queryString("_parent:p1"), fields = Seq("_parent"))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("c1", "c2") should contain(response.hits.getAt(0).id)
    response.hits.getAt(0).field("_parent").value.toString should be === ("p1")
    Set("c1", "c2") should contain(response.hits.getAt(1).id)
    response.hits.getAt(1).field("_parent").value.toString should be === ("p1")

    response = search(topChildrenQuery("child", termQuery("c_field", "yellow")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(topChildrenQuery("child", termQuery("c_field", "blue")))
    if (response.failedShards > 0) {
      logger.warn("Failed shards:")
      for (shardSearchFailure <- response.shardFailures) logger.warn("-> {}", shardSearchFailure)
    }
    response.failedShards should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(topChildrenQuery("child", termQuery("c_field", "red")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)

    response = search(hasChildQuery("child", termQuery("c_field", "yellow")))
    if (response.failedShards > 0) {
      logger.warn("Failed shards:")
      for (shardSearchFailure <- response.shardFailures) logger.warn("-> {}", shardSearchFailure)
    }
    response.failedShards should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(hasChildQuery("child", termQuery("c_field", "blue")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(hasChildQuery("child", termQuery("c_field", "red")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
    if (response.failedShards > 0) {
      logger.warn("Failed shards:")
      for (shardSearchFailure <- response.shardFailures) logger.warn("-> {}", shardSearchFailure)
    }
    response.failedShards should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)
  }

  test("_simple child query with flush") {
    indexer.putMapping(indexName, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.index(indexName, "parent", "p1", """{"p_field" : "p_value1"}""")
    indexer.index(indexName, "child", "c1", """{"c_field" : "red"}""", parent = "p1".some)
    indexer.index(indexName, "child", "c2", """{"c_field" : "yellow"}""", parent = "p1".some)
    indexer.index(indexName, "parent", "p2", """{"p_field" : "p_value2"}""")
    indexer.index(indexName, "child", "c3", """{"c_field" : "blue"}""", parent = "p2".some)
    indexer.index(indexName, "child", "c4", """{"c_field" : "red"}""", parent = "p2".some)
    indexer.flush()
    indexer.refresh()
    var response = search(topChildrenQuery("child", termQuery("c_field", "yellow")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(topChildrenQuery("child", termQuery("c_field", "blue")))
    if (response.failedShards > 0) {
      logger.warn("Failed shards:")
      for (shardSearchFailure <- response.shardFailures) logger.warn("-> {}", shardSearchFailure)
    }
    response.failedShards should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(topChildrenQuery("child", termQuery("c_field", "red")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)

    response = search(hasChildQuery("child", termQuery("c_field", "yellow")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(hasChildQuery("child", termQuery("c_field", "blue")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(hasChildQuery("child", termQuery("c_field", "red")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)
  }

  test("simple child query with flush and 3 shards") {
    val specialIndex = indexName + "_3_shards"
    indexer.createIndex(specialIndex, settings="""{"number_of_shards":3}""".some)
    indexer.putMapping(specialIndex, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.index(specialIndex, "parent", "p1", """{"p_field" : "p_value1"}""")
    indexer.index(specialIndex, "child", "c1", """{"c_field" : "red"}""", parent = "p1".some)
    indexer.index(specialIndex, "child", "c2", """{"c_field" : "yellow"}""", parent = "p1".some)
    indexer.index(specialIndex, "parent", "p2", """{"p_field" : "p_value2"}""")
    indexer.index(specialIndex, "child", "c3", """{"c_field" : "blue"}""", parent = "p2".some)
    indexer.index(specialIndex, "child", "c4", """{"c_field" : "red"}""", parent = "p2".some)
    indexer.refresh()

    var response = search(topChildrenQuery("child", termQuery("c_field", "yellow")))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(topChildrenQuery("child", termQuery("c_field", "blue")))
    if (response.failedShards > 0) {
      logger.warn("Failed shards:")
      for (shardSearchFailure <- response.shardFailures) logger.warn("-> {}", shardSearchFailure)
    }
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(topChildrenQuery("child", termQuery("c_field", "red")))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)

    response = search(hasChildQuery("child", termQuery("c_field", "yellow")))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(hasChildQuery("child", termQuery("c_field", "blue")))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(hasChildQuery("child", termQuery("c_field", "red")))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow"))))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "blue"))))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p2")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "red"))))
    //    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)
  }

  test("scoped facet") {
    indexer.putMapping(indexName, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.index(indexName, "parent", "p1", """{"p_field" : "p_value1"}""")
    indexer.index(indexName, "child", "c1", """{"c_field" : "red"}""", parent = "p1".some)
    indexer.index(indexName, "child", "c2", """{"c_field" : "yellow"}""", parent = "p1".some)
    indexer.index(indexName, "parent", "p2", """{"p_field" : "p_value2"}""")
    indexer.index(indexName, "child", "c3", """{"c_field" : "blue"}""", parent = "p2".some)
    indexer.index(indexName, "child", "c4", """{"c_field" : "red"}""", parent = "p2".some)
    indexer.refresh()
    val response = indexer.search(
      query = topChildrenQuery("child", boolQuery.should(termQuery("c_field", "red")).should(termQuery("c_field", "yellow"))).scope("child1"),
      facets = Seq(termsFacet("facet1").field("c_field").scope("child1")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (2)
    Set("p1", "p2") should contain(response.hits.getAt(0).id)
    Set("p1", "p2") should contain(response.hits.getAt(1).id)
    response.facets.facets.size should be === (1)
    val facet: TermsFacet = response.facets.facet("facet1")
    facet.entries.size should be === (2)
    facet.entries.get(0).term should be === ("red")
    facet.entries.get(0).count should be === (2)
    facet.entries.get(1).term should be === ("yellow")
    facet.entries.get(1).count should be === (1)
  }

  test("deleted parent") {
    indexer.putMapping(indexName, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.index(indexName, "parent", "p1", """{"p_field" : "p_value1"}""")
    indexer.index(indexName, "child", "c1", """{"c_field" : "red"}""", parent = "p1".some)
    indexer.index(indexName, "child", "c2", """{"c_field" : "yellow"}""", parent = "p1".some)
    indexer.index(indexName, "parent", "p2", """{"p_field" : "p_value2"}""")
    indexer.index(indexName, "child", "c3", """{"c_field" : "blue"}""", parent = "p2".some)
    indexer.index(indexName, "child", "c4", """{"c_field" : "red"}""", parent = "p2".some)
    indexer.refresh()

    // top children query

    var response = search(topChildrenQuery("child", termQuery("c_field", "yellow")))
    if (response.failedShards > 0) {
      logger.warn("Failed shards:")
      for (shardSearchFailure <- response.shardFailures) logger.warn("-> {}", shardSearchFailure)
    }
    response.failedShards should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")
    response.hits.getAt(0).sourceAsString should include(""""p_value1"""")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")
    response.hits.getAt(0).sourceAsString should include(""""p_value1"""")

    // update p1 and see what that we get updated values...

    indexer.index(indexName, "parent", "p1", """{"p_field": "p_value1_updated"}""")
    indexer.refresh()

    response = search(topChildrenQuery("child", termQuery("c_field", "yellow")))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")
    response.hits.getAt(0).sourceAsString should include(""""p_value1_updated"""")

    response = search(constantScoreQuery(hasChildFilter("child", termQuery("c_field", "yellow"))))
    shouldHaveNoFailures(response)
    response.hits.totalHits should be === (1)
    response.hits.getAt(0).id should be === ("p1")
    response.hits.getAt(0).sourceAsString should include(""""p_value1_updated"""")
  }

  test("dfs search type") {
    indexer.putMapping(indexName, "child", """{"type" : {"_parent" : {"type" : "parent"}}}""")
    indexer.index(indexName, "parent", "p1", """{"p_field": "p_value1"}""")
    indexer.index(indexName, "child", "c1", """{"c_field" : "red"}""", parent = "p1".some)
    indexer.index(indexName, "child", "c2", """{"c_field" : "yellow"}""", parent = "p1".some)
    indexer.index(indexName, "parent", "p2", """{"p_field" : "p_value2"}""")
    indexer.index(indexName, "child", "c3", """{"c_field" : "blue"}""", parent = "p2".some)
    indexer.index(indexName, "child", "c4", """{"c_field" : "red"}""", parent = "p2".some)
    indexer.refresh()
    val response = indexer.search(
      query = boolQuery.mustNot(hasChildQuery("child", boolQuery.should(queryString("c_field:*")))),
      searchType = SearchType.DFS_QUERY_THEN_FETCH.some)
    response.shardFailures.length should be === (0)
  }
}
