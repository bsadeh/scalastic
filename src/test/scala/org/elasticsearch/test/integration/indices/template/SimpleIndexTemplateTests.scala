package org.elasticsearch.test.integration.indices.template

import org.elasticsearch.index.query.QueryBuilders._
import org.scalatest._, matchers._
import scala.collection.JavaConversions._

import org.elasticsearch.action.search._
import org.elasticsearch.client._
import org.elasticsearch.common.xcontent._
import org.elasticsearch.indices._
import org.elasticsearch.test.integration._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SimpleIndexTemplateTests extends IndexerBasedTest {

  test("simpleIndexTemplateTests") {
    clean()
    indexer.putTemplate("template_1", template = Some("te*"), order = Some(0),
      mappings = Map("type1" -> """{"type1": {"properties": {"field1": {"type": "string", "store": "yes"}, "field2": {"type": "string", "store": "yes", "index", "not_analyzed"}}}}"""))
    try {
    indexer.putTemplate("template_2", template = Some("test*"), create = Some(true), order = Some(1),
      mappings = Map("type1" -> """{"type1": {"properties": {"field2": {"type": "string", "store": "no"}}}}"""))
      fail
    } catch {
      case e: IndexTemplateAlreadyExistsException =>
      case e: Exception => fail
    }
    indexer.index("test_index", "type1", "1", """{"field1": "value1", "field2": "value 2"}""", refresh = Some(true))
    indexer.waitForGreenStatus()
    var response = indexer.search(Seq("test_index"), query = termQuery("field1", "value1"), fields = Seq("field1", "field2"))
    if (response.failedShards() > 0) {
      //logger.warn("failed search " + response.shardFailures().mkString(","))
    }
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).field("field1").value().toString should be === ("value1")
    response.hits.getAt(0).field("field2").value().toString should be === ("value 2")
    indexer.index("text_index", "type1", "1", """{"field1": "value1", "field2": "value 2"}""", refresh = Some(true))
    indexer.waitForGreenStatus()
    response = indexer.search(Seq("test_index"), query = termQuery("field1", "value1"), fields = Seq("field1", "field2"))
    if (response.failedShards() > 0) {
      //logger.warn("failed search " + response.shardFailures().mkString(","))
    }
    response.failedShards() should be === (0)
    response.hits.totalHits should be === (1)
    response.hits.hits.length should be === (1)
    response.hits.getAt(0).field("field1").value().toString should be === ("value1")
    response.hits.getAt(0).field("field2").value().toString should be === ("value 2")
  }

  private def clean() {
    try {
      indexer.deleteIndex(Seq("test_index"))
    } catch {
      case e: Exception =>
    }
    try {
      indexer.deleteIndex(Seq("text_index"))
    } catch {
      case e: Exception =>
    }
    try {
      indexer.deleteTemplate("template_1")
    } catch {
      case e: Exception =>
    }
    try {
    	indexer.deleteTemplate("template_2")
    } catch {
      case e: Exception =>
    }
  }
}
