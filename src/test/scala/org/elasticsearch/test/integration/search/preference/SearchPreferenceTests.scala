package org.elasticsearch.test.integration.search.preference

import org.elasticsearch.index.query.QueryBuilders._
import org.elasticsearch.action.search._
import org.elasticsearch.client._
import org.elasticsearch.common.settings._, ImmutableSettings._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SearchPreferenceTests extends IndexerBasedTest {

  override def shouldCreateDefaultIndex = false

  test("noPreferenceRandom") {
    indexer.createIndex(indexName, settings = Map("number_of_shards" -> "1", "number_of_replicas" -> "1"))
    indexer.waitForYellowStatus()
    indexer.index(indexName, "type1", null, """{"field1": "value1"}""")
    indexer.refresh()
    
    val firstNodeId = indexer.search(indices = Seq(indexName)).getHits.getAt(0).shard().nodeId()
    val secondNodeId = indexer.search(indices = Seq(indexName)).getHits.getAt(0).shard().nodeId()
    pending //fixme: failed test - need to use 2 nodes
    firstNodeId should not be === (secondNodeId)
  }

  test("simplePreferenceTests") {
    indexer.createIndex(indexName)
    indexer.waitForYellowStatus()
    indexer.index(indexName, "type1", null, """{"field1": "value1"}""")
    indexer.refresh()
    
    indexer.search(preference = Some("_local")).getHits.totalHits should be === (1)
    indexer.search(preference = Some("_local")).getHits.totalHits should be === (1)
    indexer.search(preference = Some("_primary")).getHits.totalHits should be === (1)
    indexer.search(preference = Some("_primary")).getHits.totalHits should be === (1)
    indexer.search(preference = Some("1234")).getHits.totalHits should be === (1)
    indexer.search(preference = Some("1234")).getHits.totalHits should be === (1)
  }
}
