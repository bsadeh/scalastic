package org.elasticsearch.test.integration.indices.state

import org.scalatest._, matchers._
import org.elasticsearch.action.admin.cluster.health._
import org.elasticsearch.action.admin.cluster.state._
import org.elasticsearch.action.admin.indices.status._
import org.elasticsearch.cluster.block._
import org.elasticsearch.cluster.metadata._
import org.elasticsearch.cluster.routing._
import org.elasticsearch.common.logging._
import org.elasticsearch.test.integration._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class SimpleIndexStateTests extends IndexerBasedTest {

  test("testSimpleOpenClose") {
    //logger.info("--> starting two nodes....")
    //logger.info("--> creating test index")
    indexer.createIndex(indexName)
    //logger.info("--> waiting for green status")
    var health = indexer.health_prepare().setWaitForGreenStatus()
      .setWaitForNodes("2")
      .execute()
      .actionGet()
    health.timedOut() should be === (false)
    var stateResponse = indexer.state()
    stateResponse.state().metaData().index(indexName).state() should be === (IndexMetaData.State.OPEN)
    stateResponse.state().routingTable().index(indexName).shards().size should be === (5)
    stateResponse.state().routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size should be === (10)
    //logger.info("--> indexing a simple document")
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
    //logger.info("--> closing test index...")
    indexer.closeIndex(indexName)
    stateResponse = indexer.state()
    stateResponse.state().metaData().index(indexName).state() should be === (IndexMetaData.State.CLOSE)
    stateResponse.state().routingTable().index(indexName) should be (null)
    //logger.info("--> testing indices status api...")
    val indicesStatusResponse = indexer.status()
    //logger.info("--> trying to index into a closed index ...")
    try {
      indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
      fail("should fail")
    } catch {
      case e: ClusterBlockException =>
    }
    //logger.info("--> opening index...")
    indexer.openIndex(indexName)
    //logger.info("--> waiting for green status")
    health = indexer.health_prepare().setWaitForGreenStatus()
      .setWaitForNodes("2")
      .execute()
      .actionGet()
    health.timedOut() should be === (false)
    stateResponse = indexer.state()
    stateResponse.state().metaData().index(indexName).state() should be === (IndexMetaData.State.OPEN)
    stateResponse.state().routingTable().index(indexName).shards().size should be === (5)
    stateResponse.state().routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size should be === (10)
    //logger.info("--> indexing a simple document")
    indexer.index(indexName, "type1", "1", """{"field1": "value1"}""")
  }
}
