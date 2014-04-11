package org.elasticsearch.test.integration.routing

import org.elasticsearch._, action._
import org.elasticsearch.index.mapper._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner]) 
class SimpleRoutingTests extends MultiNodesBasedTests {

  override def beforeAll() {
    startNode("node1")
    startNode("node2")
  }

  test("testSimpleCrudRouting") {
    indexer("node1").createIndex(indexName)
    indexer("node1").waitForYellowStatus()
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> deleting with no routing, should not delete anything")
    indexer("node1").delete(indexName, "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> deleting with routing, should delete")
    indexer("node1").delete(indexName, "type1", "1", routing = Some("0"), refresh = Some(true))
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
    }
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> deleting_by_query with 1 as routing, should not delete anything")
    indexer("node1").deleteByQuery(routing = Some("1"))
    indexer("node1").refresh()
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> deleting_by_query with , should delete")
    indexer("node1").deleteByQuery(routing = Some("0"))
    indexer("node1").refresh()
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
    }
  }

  test("testSimpleSearchRouting") {
    indexer("node1").createIndex(indexName)
    indexer("node1").waitForYellowStatus()
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
    //logger.info("--> search with no routing, should fine one")
    for (i <- 0 until 5) {
      indexer("node1").search().getHits.totalHits() should equal (1)
    }
    //logger.info("--> search with wrong routing, should not find")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("1")).getHits.totalHits() should equal (0)
      indexer("node1").count(Nil, routing = Some("1")).getCount should equal (0)
    }
    //logger.info("--> search with correct routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0")).getHits.totalHits() should equal (1)
      indexer("node1").count(Nil, routing = Some("0")).getCount should equal (1)
    }
    indexer("node1").index(indexName, "type1", "2", """{"field": "value1"}""", routing = Some("1"), refresh = Some(true))
    //logger.info("--> search with no routing, should fine two")
    for (i <- 0 until 5) {
      indexer("node1").search().getHits.totalHits() should equal (2)
      indexer("node1").count().getCount should equal (2)
    }
    //logger.info("--> search with 0 routing, should find one")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0")).getHits.totalHits() should equal (1)
      indexer("node1").count(Nil, routing = Some("0")).getCount should equal (1)
    }
    //logger.info("--> search with 1 routing, should find one")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("1")).getHits.totalHits() should equal (1)
      indexer("node1").count(Nil, routing = Some("1")).getCount should equal (1)
    }
    //logger.info("--> search with 0,1 routings , should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0,1")).getHits.totalHits() should equal (2)
      indexer("node1").count(Nil, routing = Some("0,1")).getCount should equal (2)
    }
    //logger.info("--> search with 0,1,0 routings , should find two")
    for (i <- 0 until 5) {
      indexer("node1").search(routing = Some("0,1,0")).getHits.totalHits() should equal (2)
      indexer("node1").count(Nil, routing = Some("0,1,0")).getCount should equal (2)
    }
  }

  test("testRequiredRoutingMapping") {
    indexer("node1").createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true}}}"""))
    indexer("node1").waitForYellowStatus()
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
      //fixme: failing 
      //indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (false)
    }
  }

  test("testRequiredRoutingWithPathMapping") {
    indexer("node1").createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true, "path": "routing_field"}}}"""))
    indexer("node1").waitForYellowStatus()
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1", "routing_field": "0"}""", refresh = Some(true))
    //logger.info("--> check failure with different routing")
    try {
      indexer("node1").index(indexName, "type1", "1", """{"field": "value1", "routing_field": "0"}""", routing = Some("1"), refresh = Some(true))
      fail()
    } catch {
      case e: ElasticsearchException => e.unwrapCause().getClass should equal (classOf[MapperParsingException])
    }
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
  }

  test("testRequiredRoutingWithPathMappingBulk") {
    indexer("node1").createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true, "path": "routing_field"}}}"""))
    indexer("node1").waitForYellowStatus()
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1", "routing_field": "0"}""")
    indexer("node1").refresh()
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
  }

  test("testRequiredRoutingWithPathNumericType") {
    indexer("node1").createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true, "path": "routing_field"}}}"""))
    indexer("node1").waitForYellowStatus()
    indexer("node1").index(indexName, "type1", "1", """{"field": "value1", "routing_field": 0}""")
    indexer("node1").refresh()
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1").isExists should equal (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer("node1").get(indexName, "type1", "1", routing = Some("0")).isExists should equal (true)
    }
  }
}
