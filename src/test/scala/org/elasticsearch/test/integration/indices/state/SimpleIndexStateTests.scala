package org.elasticsearch.test.integration.indices.state

import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.action.admin.cluster.state._
import org.elasticsearch.action.admin.indices.status._
import org.elasticsearch.cluster.block._
import org.elasticsearch.cluster.metadata._
import org.elasticsearch.cluster.routing._
import org.elasticsearch.common.logging._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SimpleIndexStateTests extends IndexerBasedTest {

  test("testSimpleOpenClose") {
    pending //fixme: failing test
    indexer.createIndex(indexName)
    var health = indexer.health_prepare().setWaitForGreenStatus()
      .setWaitForNodes("2").execute.actionGet
    health.timedOut() should be === (false)
    var stateResponse = indexer.state()
    stateResponse.state().metaData().index(indexName).state() should be === (IndexMetaData.State.OPEN)
    stateResponse.state().routingTable().index(indexName).shards().size should be === (5)
    stateResponse.state().routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size should be === (10)
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    indexer.closeIndex(indexName)
    stateResponse = indexer.state()
    stateResponse.state().metaData().index(indexName).state() should be === (IndexMetaData.State.CLOSE)
    stateResponse.state().routingTable().index(indexName) should be (null)
    //logger.info("--> testing indices status api...")
    val indicesStatusResponse = indexer.status()
    //logger.info("--> trying to index into a closed index ...")
    try {
      indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
      fail
    } catch {
      case e: ClusterBlockException =>
    }
    indexer.openIndex(indexName)
    health = indexer.health_prepare().setWaitForGreenStatus()
      .setWaitForNodes("2").execute.actionGet
    health.timedOut() should be === (false)
    stateResponse = indexer.state()
    stateResponse.state().metaData().index(indexName).state() should be === (IndexMetaData.State.OPEN)
    stateResponse.state().routingTable().index(indexName).shards().size should be === (5)
    stateResponse.state().routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size should be === (10)
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
  }
}
