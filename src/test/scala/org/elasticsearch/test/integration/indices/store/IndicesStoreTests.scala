package org.elasticsearch.test.integration.indices.store

import org.elasticsearch.env._
import org.elasticsearch.index.shard._
import org.elasticsearch.node.internal._
import com.traackr.scalastic.elasticsearch._

@org.junit.runner.RunWith(classOf[org.scalatest.junit.JUnitRunner])class IndicesStoreTests extends MultiNodesBasedTests {

  override def defaultSettings = super.defaultSettings + ("gateway.type" -> "local")
  
  override def beforeAll() {
    startNode("server1")
    startNode("server2")
  }

  test("shardsCleanup") {
    indexer("server1").createIndex(indexName, settings = Map("numberOfReplicas" -> "1", "numberOfShards" -> "1"))
    indexer("server1").waitForYellowStatus()
    //logger.info("--> making sure that shard and it's replica are allocated on server1 and server2")
    shardDirectory("server1", indexName, 0).exists() should be === (true)
    shardDirectory("server2", indexName, 0).exists() should be === (true)
    //logger.info("--> starting node server3")
    startNode("server3")
    //logger.info("--> making sure that shard is not allocated on server3")
    shardDirectory("server3", indexName, 0).exists() should be === (false)
    val server2Shard = shardDirectory("server2", indexName, 0)
    //logger.info("--> stopping node server2")
    closeNode("server2")
    server2Shard.exists() should be === (true)
    indexer("server1").waitForYellowStatus()
    //logger.info("--> making sure that shard and it's replica exist on server1, server2 and server3")
    shardDirectory("server1", indexName, 0).exists() should be === (true)
    server2Shard.exists() should be === (true)
    shardDirectory("server3", indexName, 0).exists() should be === (true)
    //logger.info("--> starting node server2")
    startNode("server2")
    indexer("server1").waitForYellowStatus()
    //logger.info("--> making sure that shard and it's replica are allocated on server1 and server3 but not on server2")
    shardDirectory("server1", indexName, 0).exists() should be === (true)
    shardDirectory("server2", indexName, 0).exists() should be === (false)
    shardDirectory("server3", indexName, 0).exists() should be === (true)
  }

  private def shardDirectory(server: String, index: String, shard: Int) = {
    val node = super.node(server).asInstanceOf[InternalNode]
    val env = node.injector().getInstance(classOf[NodeEnvironment])
    env.shardLocations(new ShardId(index, shard))(0)
  }
}
