package org.elasticsearch.test.integration.routing

import org.elasticsearch.index.query.QueryBuilders._
import org.scalatest._, matchers._
import org.elasticsearch._
import org.elasticsearch.action._
import org.elasticsearch.client._
import org.elasticsearch.common.xcontent._
import org.elasticsearch.index.mapper._
import org.elasticsearch.index.query._
import org.elasticsearch.test.integration._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SimpleRoutingTests extends IndexerBasedTest {

  test("testSimpleCrudRouting") {
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> deleting with no routing, should not delete anything")
    indexer.delete(indexName, "type1", "1", refresh = Some(true))
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> deleting with routing, should delete")
    indexer.delete(indexName, "type1", "1", routing = Some("0"), refresh = Some(true))
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (false)
    }
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> deleting_by_query with 1 as routing, should not delete anything")
    indexer.deleteByQuery(routing = Some("1"))
    indexer.refresh()
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> deleting_by_query with , should delete")
    indexer.deleteByQuery(routing = Some("0"))
    indexer.refresh()
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (false)
    }
  }

  test("testSimpleSearchRouting") {
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
    //logger.info("--> search with no routing, should fine one")
    for (i <- 0 until 5) {
      indexer.search().hits.totalHits() should be === (1)
    }
    //logger.info("--> search with wrong routing, should not find")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("1")).hits.totalHits() should be === (0)
      indexer.count(Nil, routing = Some("1")).count() should be === (0)
    }
    //logger.info("--> search with correct routing, should find")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("0")).hits.totalHits() should be === (1)
      indexer.count(Nil, routing = Some("0")).count() should be === (1)
    }
    //logger.info("--> indexing with id [2], and routing [1]")
    indexer.index(indexName, "type1", "2", """{"field": "value1"}""", routing = Some("1"), refresh = Some(true))
    //logger.info("--> search with no routing, should fine two")
    for (i <- 0 until 5) {
      indexer.search().hits.totalHits() should be === (2)
      indexer.count().count should be === (2)
    }
    //logger.info("--> search with 0 routing, should find one")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("o")).hits.totalHits() should be === (1)
      indexer.count(Nil, routing = Some("0")).count() should be === (1)
    }
    //logger.info("--> search with 1 routing, should find one")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("1")).hits.totalHits() should be === (1)
      indexer.count(Nil, routing = Some("1")).count() should be === (1)
    }
    //logger.info("--> search with 0,1 routings , should find two")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("0"), types = Seq("1")).hits.totalHits() should be === (2)
      indexer.count(Nil, routing = Some("0,1")).count() should be === (2)
    }
    //logger.info("--> search with 0,1,0 routings , should find two")
    for (i <- 0 until 5) {
      indexer.search(routing = Some("0,1"), types = Seq("0")).hits.totalHits() should be === (2)
      indexer.count(Nil, routing = Some("0,1,0")).count() should be === (2)
    }
  }

  test("testRequiredRoutingMapping") {
    try {
      indexer.deleteIndex(Seq(indexName))
    } catch {
      case e: Exception =>
    }
    indexer.createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true}}}"""))
    indexer.waitForGreenStatus()
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1"}""", routing = Some("0"), refresh = Some(true))
    //logger.info("--> verifying get with no routing, should not find anything")
    //logger.info("--> indexing with id [1], with no routing, should fail")
    try {
      indexer.index(indexName, "type1", "1", "field", "value1", refresh = Some(true))
      fail("should fail")
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

  test("testRequiredRoutingWithPathMapping") {
    try {
      indexer.deleteIndex(Seq(indexName))
    } catch {
      case e: Exception =>
    }
    indexer.createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true, "path": "routing_field"}}}"""))
    indexer.waitForGreenStatus()
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1", "routing_field": "0"}""", refresh = Some(true))
    //logger.info("--> check failure with different routing")
    try {
      indexer.index(indexName, "type1", "1", """{"field", "value1", "routing_field": "0"}""", routing = Some("1"), refresh = Some(true))
      fail("should fail")
    } catch {
      case e: ElasticSearchException => e.unwrapCause().getClass should be === classOf[MapperParsingException]
    }
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
  }

  test("testRequiredRoutingWithPathMappingBulk") {
    try {
      indexer.deleteIndex(Seq(indexName))
    } catch {
      case e: Exception =>
    }
    indexer.createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true, "path": "routing_field"}}}"""))
    indexer.waitForGreenStatus()
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1", "routing_field": "0")}""")
    indexer.refresh()
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
  }

  test("testRequiredRoutingWithPathNumericType") {
    indexer.deleteIndex()
    indexer.createIndex(indexName, mappings = Map("type1" -> """{"type1": {"_routing": {"required": true, "path": "routing_field"}}}"""))
    indexer.waitForGreenStatus()
    //logger.info("--> indexing with id [1], and routing [0]")
    indexer.index(indexName, "type1", "1", """{"field": "value1", "routing_field": 0}""")
    indexer.refresh()
    //logger.info("--> verifying get with no routing, should not find anything")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1").exists() should be === (false)
    }
    //logger.info("--> verifying get with routing, should find")
    for (i <- 0 until 5) {
      indexer.get(indexName, "type1", "1", routing = Some("0")).exists() should be === (true)
    }
  }
}
