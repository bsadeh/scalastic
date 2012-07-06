package org.elasticsearch.test.integration.routing

import org.elasticsearch.cluster.metadata.AliasAction._
import org.elasticsearch.index.query.QueryBuilders._
import org.scalatest._, matchers._
import org.elasticsearch._
import org.elasticsearch.action._
import org.elasticsearch.client._
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.query._
import org.elasticsearch.test.integration._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class AliasRoutingTests extends IndexerBasedTest {

  test("testAliasCrudRouting") {
    indexer.alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias0").routing("0")))
    //logger.info("--> indexing with id [1], and routing [0] using alias")
    indexer.index("alias0", "type1", "1", "field", "value1", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> verifying get with routing alias, should find")
    for (i <- 0 until 5) {
      indexer.get("alias0", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> deleting with no routing, should not delete anything")
    indexer.delete(indexName, "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
      indexer.get("alias0", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> deleting with routing alias, should delete")
    indexer.delete("alias0", "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (false)
      indexer.get("alias0", "type1", "1").exists() should be === (false)
    }
    //logger.info("--> indexing with id [1], and routing [0] using alias")
    indexer.index("alias0", "type1", "1", "field", "value1", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
      indexer.get("alias0", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> deleting_by_query with 1 as routing, should not delete anything")
    indexer.deleteByQuery(routing = Some("1"))
    indexer.refresh()
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
      indexer.get("alias0", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> deleting_by_query with alias0, should delete")
    indexer.deleteByQuery(Seq("alias0"), query = matchAllQuery)
    indexer.refresh()
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (false)
      indexer.get("alias0", "type1", "1").exists() should be === (false)
    }
  }

  test("testAliasSearchRouting") {
    indexer.alias(Seq("test1"), "alias", actions = Seq(
      newAddAliasAction(indexName, "alias0").routing("0"),
      newAddAliasAction(indexName, "alias1").routing("1"),
      newAddAliasAction(indexName, "alias01").routing("0,1")))
    //logger.info("--> indexing with id [1], and routing [0] using alias")
    indexer.index("alias0", "type1", "1", "field", "value1", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get("alias0", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> search with no routing, should fine one")
    for (i <- 0 until 5) {
      indexer.search().hits.totalHits() should be === (1)
    }
    //logger.info("--> search with wrong routing, should not find")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("1")).hits.totalHits() should be === (0)
      indexer.count(Nil, routing = Some("1")).count() should be === (0)
      indexer.search(indices = Seq("alias1")).hits.totalHits() should be === (0)
      indexer.count(Seq("alias1")).count() should be === (0)
    }
    //logger.info("--> search with correct routing, should find")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("0")).hits.totalHits() should be === (1)
      indexer.count(Nil, routing = Some("0")).count() should be === (1)
      indexer.search(indices = Seq("alias0")).hits.totalHits() should be === (1)
      indexer.count(Seq("alias0")).count() should be === (1)
    }
    //logger.info("--> indexing with id [2], and routing [1] using alias")
    indexer.index("alias1", "type1", "2", "field", "value1", refresh = Some(true))
    //logger.info("--> search with no routing, should fine two")
    for (i <- 0 until 5) {
      indexer.search().hits.totalHits() should be === (2)
      indexer.count().count should be === (2)
    }
    //logger.info("--> search with 0 routing, should find one")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("0")).hits.totalHits() should be === (1)
      indexer.count(Nil, routing = Some("0")).count() should be === (1)
      indexer.search(indices = Seq("alias0")).hits.totalHits() should be === (1)
      indexer.count(Seq("alias0")).count() should be === (1)
    }
    //logger.info("--> search with 1 routing, should find one")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("1")).hits.totalHits() should be === (1)
      indexer.count(Nil, routing = Some("1")).count() should be === (1)
      indexer.search(indices = Seq("alias1")).hits.totalHits() should be === (1)
      indexer.count(Seq("alias1")).count() should be === (1)
    }
    //logger.info("--> search with 0,1 routings , should find two")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("0"), types = Seq("1")).hits.totalHits() should be === (2)
      indexer.count(Nil, routing = Some("0,1")).count() should be === (2)
      indexer.search(indices = Seq("alias01")).hits.totalHits() should be === (2)
      indexer.count(Seq("alias01")).count() should be === (2)
    }
    //logger.info("--> search with two routing aliases , should find two")
    for (i <- 0 until 5) {
      indexer.search(indices = Seq("alias0"), types = Seq("alias1")).hits.totalHits() should be === (2)
      indexer.count(Seq("alias0", "alias1")).count() should be === (2)
    }
    //logger.info("--> search with alias0, alias1 and alias01, should find two")
    for (i <- 0 until 5) {
      indexer.search(Seq("alias0", "alias1", "alias01")).hits.totalHits() should be === (2)
      indexer.count(Seq("alias0", "alias1", "alias01")).count() should be === (2)
    }
    //logger.info("--> search with test, alias0 and alias1, should find two")
    for (i <- 0 until 5) {
      indexer.search(indices = Seq(indexName, "alias0"), types = Seq("alias1")).hits.totalHits() should be === (2)
      indexer.count(Seq(indexName, "alias0", "alias1")).count() should be === (2)
    }
  }

  test("testAliasSearchRoutingWithTwoIndices") {
    indexer.createIndex("test-a")
    indexer.createIndex("test-b")
    indexer.alias(Nil, "", actions = Seq(
      newAddAliasAction("test-a", "alias-a0").routing("0"),
      newAddAliasAction("test-a", "alias-a1").routing("1"),
      newAddAliasAction("test-b", "alias-b0").routing("0"),
      newAddAliasAction("test-b", "alias-b1").routing("1"),
      newAddAliasAction("test-a", "alias-ab").searchRouting("0"),
      newAddAliasAction("test-b", "alias-ab").searchRouting("1")))
    indexer.alias(Seq("test1"), "alias", actions = Seq(
      newAddAliasAction("test1", "alias10").routing("0"),
      newAddAliasAction("test2", "alias20").routing("0"),
      newAddAliasAction("test2", "alias21").routing("1"),
      newAddAliasAction("test1", "alias0").routing("0"),
      newAddAliasAction("test2", "alias0").routing("0")))
    //logger.info("--> indexing with id [1], and routing [0] using alias to test-a")
    indexer.index("alias-a0", "type1", "1", "field", "value1", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get("alias-a0", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> indexing with id [0], and routing [1] using alias to test-b")
    indexer.index("alias-b1", "type1", "1", "field", "value1", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get("alias-b1", "type1", "1").exists() should be === (true)
    }
    //logger.info("--> search with alias-a1,alias-b0, should not find")
    for (i <- 0 until 5) {
      indexer.search(indices = Seq("alias-a1"), types = Seq("alias-b0")).hits.totalHits() should be === (0)
      indexer.count(Seq("alias-a1", "alias-b0")).count() should be === (0)
    }
    //logger.info("--> search with alias-ab, should find two")
    for (i <- 0 until 5) {
      indexer.search(indices = Seq("alias-ab")).hits.totalHits() should be === (2)
      indexer.count(Seq("alias-ab")).count() should be === (2)
    }
    //logger.info("--> search with alias-a0,alias-b1 should find two")
    for (i <- 0 until 5) {
      indexer.search(indices = Seq("alias-a0"), types = Seq("alias-b1")).hits.totalHits() should be === (2)
      indexer.count(Seq("alias-a0", "alias-b1")).count() should be === (2)
    }
  }

  test("testRequiredRoutingMappingWithAlias") {
    indexer.putMapping(indexName, "type1", """{"type1": {"_routing": {"required": true}}}""")
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> indexing with id [1], with no routing, should fail")
    try {
      indexer.index(indexName, "type1", "1", "field", "value1", refresh = Some(true))
      fail
    } catch {
      case e: ElasticSearchException => e.unwrapCause().getClass should be === classOf[RoutingMissingException]
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> deleting with no routing, should broadcast the delete since _routing is required")
    indexer.delete(indexName, "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (false)
    }
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> bulk deleting with no routing, should broadcast the delete since _routing is required")
    indexer.bulk(Seq(indexer.delete_prepare(indexName, "type1", "1").request))
    indexer.refresh()
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (false)
    }
  }

  test("testIndexingAliasesOverTime") {
    //logger.info("--> creating alias with routing [3]")
    indexer.alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias").routing("3")))
    //logger.info("--> indexing with id [0], and routing [3]")
    indexer.index("alias", "type1", "0", "field", "value1", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> verifying get and search with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "0", routing = Some("3")).exists() should be === (true)
      indexer.search(indices = Seq("alias")).hits.totalHits() should be === (1)
      indexer.count(Seq("alias")).count() should be === (1)
    }
    //logger.info("--> creating alias with routing [4]")
    indexer.alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias").routing("4")))
    //logger.info("--> verifying search with wrong routing should not find")
    for (i <- 0 until 5) {
      indexer.search(indices = Seq("alias")).hits.totalHits() should be === (0)
      indexer.count(Seq("alias")).count() should be === (0)
    }
    //logger.info("--> creating alias with search routing [3,4] and index routing 4")
    indexer.alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias").searchRouting("3,4").indexRouting("4")))
    //logger.info("--> indexing with id [1], and routing [4]")
    indexer.index("alias", "type1", "1", "field", "value2", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> verifying get and search with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "0", routing = Some("3")).exists() should be === (true)
      indexer.get(indexName, "type1", "1", routing = Some("4")).exists() should be === (true)
      indexer.search(indices = Seq("alias")).hits.totalHits() should be === (2)
      indexer.count(Seq("alias")).count() should be === (2)
    }
  }
}
