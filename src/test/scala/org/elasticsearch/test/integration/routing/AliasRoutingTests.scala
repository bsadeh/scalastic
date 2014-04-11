package org.elasticsearch.test.integration.routing

import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch._, action._
import org.elasticsearch.cluster.metadata.AliasAction._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class AliasRoutingTests extends MultiNodesBasedTests {

  override def beforeAll() {
    startNode("node1")
    startNode("node2")
  }

  test("testAliasCrudRouting") {
    indexer("node1").createIndex(indexName)
    indexer("node1").waitForYellowStatus()
    indexer("node1").alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias0").routing("0")))
    indexer("node1").index("alias0", "type1", "1", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> verifying get with routing alias, should find")
    for (i <- 0 until 5) {
      indexer("node1").get("alias0", "type1", "1").isExists should equal (true)
    }
    //logger.info("--> deleting with no routing, should not delete anything")
    indexer("node1").delete(indexName, "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
      indexer("node1").get("alias0", "type1", "1").isExists should equal (true)
    }
    //logger.info("--> deleting with routing alias, should delete")
    indexer("node1").delete("alias0", "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
      indexer("node1").get("alias0", "type1", "1").isExists should equal (false)
    }
    indexer("node1").index("alias0", "type1", "1", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
      indexer("node1").get("alias0", "type1", "1").isExists should equal (true)
    }
    //logger.info("--> deleting_by_query with 1 as routing, should not delete anything")
    indexer("node1").deleteByQuery(routing = Some("1"))
    indexer("node1").refresh()
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
      indexer("node1").get("alias0", "type1", "1").isExists should equal (true)
    }
    //logger.info("--> deleting_by_query with alias0, should delete")
    indexer("node1").deleteByQuery(Seq("alias0"), query = matchAllQuery)
    indexer("node1").refresh()
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
      indexer("node1").get("alias0", "type1", "1").isExists should equal (false)
    }
  }

  test("testAliasSearchRouting") {
    indexer("node1").createIndex(indexName)
    indexer("node1").waitForYellowStatus()
    indexer("node1").alias(Nil, "", actions = Seq(
      newAddAliasAction(indexName, "alias"),
      newAddAliasAction(indexName, "alias0").routing("0"),
      newAddAliasAction(indexName, "alias1").routing("1")))
    indexer("node1").index("alias0", "type1", "1", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get("alias0", "type1", "1").isExists should equal (true)
    }
    //logger.info("--> search with no routing, should fine one")
    for (i <- 0 until 5) {
      indexer("node1").search().getHits.totalHits() should equal (1)
    }
    //logger.info("--> search with wrong routing, should not find")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("1")).getHits.totalHits() should equal (0)
      indexer("node1").count(Nil, routing = Some("1")).getCount should equal (0)
      indexer("node1").search(indices = Seq("alias1")).getHits.totalHits() should equal (0)
      indexer("node1").count(Seq("alias1")).getCount should equal (0)
    }
    //logger.info("--> search with correct routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0")).getHits.totalHits() should equal (1)
      indexer("node1").count(Nil, routing = Some("0")).getCount should equal (1)
      indexer("node1").search(indices = Seq("alias0")).getHits.totalHits() should equal (1)
      indexer("node1").count(Seq("alias0")).getCount should equal (1)
    }
    indexer("node1").index("alias1", "type1", "2", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> search with no routing, should fine two")
    for (i <- 0 until 5) {
      indexer("node1").search().getHits.totalHits() should equal (2)
      indexer("node1").count().getCount should equal (2)
    }
    //logger.info("--> search with 0 routing, should find one")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0")).getHits.totalHits() should equal (1)
      indexer("node1").count(Nil, routing = Some("0")).getCount should equal (1)
      indexer("node1").search(indices = Seq("alias0")).getHits.totalHits() should equal (1)
      indexer("node1").count(Seq("alias0")).getCount should equal (1)
    }
    //logger.info("--> search with 1 routing, should find one")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("1")).getHits.totalHits() should equal (1)
      indexer("node1").count(Nil, routing = Some("1")).getCount should equal (1)
      indexer("node1").search(indices = Seq("alias1")).getHits.totalHits() should equal (1)
      indexer("node1").count(Seq("alias1")).getCount should equal (1)
    }
    //logger.info("--> search with 0,1 routings , should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0,1")).getHits.totalHits() should equal (2)
      indexer("node1").count(Nil, routing = Some("0,1")).getCount should equal (2)
    }
    //logger.info("--> search with two routing aliases , should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(indices = Seq("alias0","alias1")).getHits.totalHits() should equal (2)
      indexer("node1").count(Seq("alias0", "alias1")).getCount should equal (2)
    }
    //logger.info("--> search with test, alias0 and alias1, should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(indices = Seq(indexName, "alias0","alias1")).getHits.totalHits() should equal (2)
      indexer("node1").count(Seq(indexName, "alias0", "alias1")).getCount should equal (2)
    }
  }

  test("testAliasSearchRoutingWithTwoIndices") {
    indexer("node1").createIndex("test-a")
    indexer("node1").createIndex("test-b")
    indexer("node1").waitForYellowStatus()
    indexer("node1").alias(Nil, "", actions = Seq(
      newAddAliasAction("test-a", "alias-a0").routing("0"),
      newAddAliasAction("test-a", "alias-a1").routing("1"),
      newAddAliasAction("test-b", "alias-b0").routing("0"),
      newAddAliasAction("test-b", "alias-b1").routing("1"),
      newAddAliasAction("test-a", "alias-ab").searchRouting("0"),
      newAddAliasAction("test-b", "alias-ab").searchRouting("1")))
    indexer("node1").index("alias-a0", "type1", "1", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //    for (i <- 0 until 5) indexer("node1").get("test", "type1", "1").isExists should equal (false)
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) indexer("node1").get("alias-a0", "type1", "1").isExists should equal (true)
    indexer("node1").index("alias-b1", "type1", "1", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //    for (i <- 0 until 5) indexer("node1").get("test", "type1", "1").isExists should equal (false)
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) indexer("node1").get("alias-b1", "type1", "1").isExists should equal (true)
    //logger.info("--> search with alias-a1,alias-b0, should not find")
    for (i <- 0 until 5) {
      indexer("node1").search(indices = Seq("alias-a1", "alias-b0")).getHits.totalHits() should equal (0)
      indexer("node1").count(Seq("alias-a1", "alias-b0")).getCount should equal (0)
    }
    //logger.info("--> search with alias-ab, should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(indices = Seq("alias-ab")).getHits.totalHits() should equal (2)
      indexer("node1").count(Seq("alias-ab")).getCount should equal (2)
    }
    //logger.info("--> search with alias-a0,alias-b1 should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(indices = Seq("alias-a0", "alias-b1")).getHits.totalHits() should equal (2)
      indexer("node1").count(Seq("alias-a0", "alias-b1")).getCount should equal (2)
    }
  }

  test("testRequiredRoutingMappingWithAlias") {
    pending //originally ignored
    indexer("node1").putMapping(indexName, "type1", """{"type1": {"_routing": {"required": true}}}""")
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    try {
      indexer("node1").index(indexName, "type1", "1", """{"field": "value1"}""", refresh = Some(true))
      fail()
    } catch {
      case e: ElasticsearchException => e.unwrapCause().getClass should equal (classOf[RoutingMissingException])
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> deleting with no routing, should broadcast the delete since _routing is required")
    indexer("node1").delete(indexName, "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
    }
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> bulk deleting with no routing, should broadcast the delete since _routing is required")
    indexer("node1").bulk(Seq(indexer("node1").delete_prepare(indexName, "type1", "1").request))
    indexer("node1").refresh()
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
    }
  }

  test("testIndexingAliasesOverTime") {
    indexer("node1").createIndex(indexName)
    indexer("node1").waitForYellowStatus()
    //logger.info("--> creating alias with routing [3]")
    indexer("node1").alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias").routing("3")))
    indexer("node1").index("alias", "type1", "0", """{"field": "value1"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> verifying get and search with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "0", routing = Some("3")).isExists should equal (true)
      indexer("node1").search(indices = Seq("alias")).getHits.totalHits() should equal (1)
      indexer("node1").count(Seq("alias")).getCount should equal (1)
    }
    //logger.info("--> creating alias with routing [4]")
    indexer("node1").alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias").routing("4")))
    //logger.info("--> verifying search with wrong routing should not find")
    for (i <- 0 until 5) {
      indexer("node1").search(indices = Seq("alias")).getHits.totalHits() should equal (0)
      indexer("node1").count(Seq("alias")).getCount should equal (0)
    }
    //logger.info("--> creating alias with search routing [3,4] and index routing 4")
    indexer("node1").alias(Nil, "", actions = Seq(newAddAliasAction(indexName, "alias").searchRouting("3,4").indexRouting("4")))
    indexer("node1").index("alias", "type1", "1", """{"field": "value2"}""", refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> verifying get and search with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "0", routing = Some("3")).isExists should equal (true)
      indexer("node1").get(indexName, "type1", "1", routing = Some("4")).isExists should equal (true)
      indexer("node1").search(indices = Seq("alias")).getHits.totalHits() should equal (2)
      indexer("node1").count(Seq("alias")).getCount should equal (2)
    }
  }
}
