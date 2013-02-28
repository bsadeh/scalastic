package org.elasticsearch.test.integration.indices.state

import org.elasticsearch.cluster.block._
import org.elasticsearch.cluster.metadata._
import org.elasticsearch.cluster.routing._
import scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])
class SimpleIndexStateTests extends IndexerBasedTest {

  test("testSimpleOpenClose") {
    pending //fixme: failing test
    indexer.createIndex(indexName)
    var health = indexer.health_prepare().setWaitForGreenStatus()
      .setWaitForNodes("2").execute.actionGet
    health.isTimedOut should be === (false)
    var stateResponse = indexer.state()
    stateResponse.getState.metaData().index(indexName).state() should be === (IndexMetaData.State.OPEN)
    stateResponse.getState.routingTable().index(indexName).shards().size should be === (5)
    stateResponse.getState.routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size should be === (10)
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.closeIndex(indexName)
    stateResponse = indexer.state()
    stateResponse.getState.metaData().index(indexName).state() should be === (IndexMetaData.State.CLOSE)
    stateResponse.getState.routingTable().index(indexName) should be (null)

    try {
      indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
      fail()
    } catch {
      case e: ClusterBlockException =>
    }
    indexer.openIndex(indexName)
    health = indexer.health_prepare().setWaitForGreenStatus()
      .setWaitForNodes("2").execute.actionGet
    health.isTimedOut should be === (false)
    stateResponse = indexer.state()
    stateResponse.getState.metaData().index(indexName).state() should be === (IndexMetaData.State.OPEN)
    stateResponse.getState.routingTable().index(indexName).shards().size should be === (5)
    stateResponse.getState.routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size should be === (10)
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
  }
}
